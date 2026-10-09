/**
 * Metrolist Project (C) 2026
 * Licensed under GPL-3.0 | See git history for contributors
 */

package com.metrolist.music.lyrics

import android.content.Context
import android.media.session.PlaybackState
import android.os.SystemClock
import androidx.media3.common.PlaybackParameters
import androidx.media3.common.Player
import com.metrolist.music.db.MusicDatabase
import com.metrolist.music.db.entities.LyricsEntity
import com.metrolist.music.models.MediaMetadata
import io.github.proify.lyricon.lyric.model.LyricWord
import io.github.proify.lyricon.lyric.model.RichLyricLine
import io.github.proify.lyricon.lyric.model.Song
import io.github.proify.lyricon.provider.LyriconFactory
import io.github.proify.lyricon.provider.LyriconProvider
import io.github.proify.lyricon.provider.service.addConnectionListener
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.distinctUntilChangedBy
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import timber.log.Timber

/**
 * Mirrors the current song, its synced lyrics and the playback state to Lyricon
 * (https://github.com/tomakino/lyricon), an Xposed module that renders lyrics in the
 * status bar, through its Provider SDK.
 *
 * Everything here runs on the main thread (the [scope] of [com.metrolist.music.playback.MusicService]),
 * which is also the ExoPlayer application thread.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class StatusBarLyricsPublisher(
    private val context: Context,
    private val scope: CoroutineScope,
    private val database: MusicDatabase,
    private val playerFlow: StateFlow<Player?>,
    private val currentMediaMetadata: StateFlow<MediaMetadata?>,
    enabledFlow: Flow<Boolean>,
) {
    private var provider: LyriconProvider? = null
    private var attachedPlayer: Player? = null
    private var currentSong: Song? = null
    private var positionSyncJob: Job? = null

    private val playerListener =
        object : Player.Listener {
            override fun onIsPlayingChanged(isPlaying: Boolean) = publishPlaybackState()

            override fun onPlaybackStateChanged(playbackState: Int) = publishPlaybackState()

            override fun onPlaybackParametersChanged(playbackParameters: PlaybackParameters) = publishPlaybackState()

            override fun onPositionDiscontinuity(
                oldPosition: Player.PositionInfo,
                newPosition: Player.PositionInfo,
                reason: Int,
            ) {
                if (reason == Player.DISCONTINUITY_REASON_SEEK) {
                    attachedPlayer?.let { provider?.player?.seekTo(it.currentPosition.coerceAtLeast(0L)) }
                }
                publishPlaybackState()
            }
        }

    private val job: Job =
        scope.launch {
            enabledFlow.distinctUntilChanged().collectLatest { enabled ->
                if (!enabled) return@collectLatest
                start()
                try {
                    coroutineScope {
                        launch { playerFlow.collect { attach(it) } }
                        launch {
                            currentMediaMetadata
                                .distinctUntilChangedBy { it?.id }
                                .flatMapLatest { metadata ->
                                    if (metadata == null) {
                                        flowOf(null)
                                    } else {
                                        database.lyrics(metadata.id).map { metadata to it }
                                    }
                                }
                                .distinctUntilChanged()
                                .collect { publishSong(it?.first, it?.second) }
                        }
                    }
                } finally {
                    stop()
                }
            }
        }

    /** Stops publishing and releases the connection to Lyricon. Safe to call more than once. */
    fun release() {
        job.cancel()
        stop()
    }

    private fun start() {
        if (provider != null) return
        provider =
            runCatching {
                LyriconFactory.createProvider(context = context).also { created ->
                    created.autoSync = true
                    created.service.addConnectionListener {
                        onConnected { Timber.tag(TAG).i("Connected to Lyricon") }
                        onReconnected { Timber.tag(TAG).i("Reconnected to Lyricon") }
                        onDisconnected { Timber.tag(TAG).i("Disconnected from Lyricon") }
                        onConnectTimeout { Timber.tag(TAG).w("Lyricon central service not reachable (module inactive?)") }
                    }
                    created.register()
                }
            }.onFailure { Timber.tag(TAG).e(it, "Failed to create Lyricon provider") }
                .getOrNull()
    }

    private fun stop() {
        positionSyncJob?.cancel()
        positionSyncJob = null
        attachedPlayer?.removeListener(playerListener)
        attachedPlayer = null
        currentSong = null
        provider?.let { active ->
            runCatching {
                active.player.setSong(null)
                active.player.setPlaybackState(false)
                active.unregister()
                active.destroy()
            }.onFailure { Timber.tag(TAG).w(it, "Failed to release Lyricon provider") }
        }
        provider = null
    }

    private fun attach(player: Player?) {
        if (attachedPlayer === player) return
        attachedPlayer?.removeListener(playerListener)
        attachedPlayer = player
        player?.addListener(playerListener)
        publishPlaybackState()
    }

    private fun publishSong(metadata: MediaMetadata?, entity: LyricsEntity?) {
        val remote = provider?.player ?: return
        if (metadata == null) {
            currentSong = null
            remote.setSong(null)
            remote.setPlaybackState(false)
            return
        }
        val durationMs = metadata.duration.toLong() * 1000L
        val lines =
            entity?.lyrics
                ?.takeIf { it.isNotBlank() && it != LyricsEntity.LYRICS_NOT_FOUND }
                ?.let(LyricsUtils::parseLyrics)
                .orEmpty()
                .toRichLines(durationMs)
        val song =
            Song(
                id = metadata.id,
                name = metadata.title,
                artist = metadata.artists.joinToString { it.name },
                duration = durationMs,
                lyrics = lines.takeIf { it.isNotEmpty() },
            )
        currentSong = song
        remote.setSong(song)
        attachedPlayer?.let { remote.seekTo(it.currentPosition.coerceAtLeast(0L)) }
        publishPlaybackState()
    }

    private fun publishPlaybackState() {
        val remote = provider?.player ?: return
        val player = attachedPlayer
        if (player == null || currentSong == null) {
            remote.setPlaybackState(false)
            restartPositionSync(playing = false)
            return
        }
        remote.setPlaybackState(buildPlaybackState(player))
        restartPositionSync(playing = player.isPlaying)
    }

    /**
     * Lyricon extrapolates the position from [PlaybackState] (position, speed, update time), so
     * a periodic refresh is only needed to correct drift.
     */
    private fun restartPositionSync(playing: Boolean) {
        positionSyncJob?.cancel()
        positionSyncJob = null
        if (!playing) return
        positionSyncJob =
            scope.launch {
                while (isActive) {
                    delay(POSITION_SYNC_INTERVAL_MS)
                    val player = attachedPlayer ?: break
                    val remote = provider?.player ?: break
                    if (!player.isPlaying) break
                    remote.setPlaybackState(buildPlaybackState(player))
                }
            }
    }

    private fun buildPlaybackState(player: Player): PlaybackState {
        val playing = player.isPlaying
        return PlaybackState
            .Builder()
            .setState(
                if (playing) PlaybackState.STATE_PLAYING else PlaybackState.STATE_PAUSED,
                player.currentPosition.coerceAtLeast(0L),
                if (playing) player.playbackParameters.speed else 0f,
                SystemClock.elapsedRealtime(),
            ).build()
    }

    private fun List<LyricsEntry>.toRichLines(durationMs: Long): List<RichLyricLine> {
        val main = filter { !it.isBackground && it.text.isNotBlank() }
        return main.mapIndexed { index, entry ->
            val nextStart = main.getOrNull(index + 1)?.time
            val end =
                when {
                    nextStart != null && nextStart > entry.time -> nextStart
                    durationMs > entry.time -> durationMs
                    else -> entry.time + FALLBACK_LINE_DURATION_MS
                }
            RichLyricLine(
                begin = entry.time,
                end = end,
                text = entry.text,
                words =
                    entry.words
                        ?.map { word ->
                            LyricWord(
                                begin = (word.startTime * 1000).toLong(),
                                end = (word.endTime * 1000).toLong(),
                                text = if (word.hasTrailingSpace) word.text + " " else word.text,
                            )
                        }?.takeIf { it.isNotEmpty() },
                translation = entry.translatedTextFlow.value?.takeIf { it.isNotBlank() },
                roma = entry.romanizedTextFlow.value?.takeIf { it.isNotBlank() },
            )
        }
    }

    companion object {
        private const val TAG = "StatusBarLyrics"
        private const val POSITION_SYNC_INTERVAL_MS = 10_000L
        private const val FALLBACK_LINE_DURATION_MS = 5_000L
    }
}
