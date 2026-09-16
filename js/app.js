/* ============================================================
   Music Together — P2P Real-time Sync Engine
   ============================================================ */

(() => {
    'use strict';

    // ─── CONFIG ────────────────────────────────────────────
    const SYNC_INTERVAL_MS = 500;
    const SYNC_THRESHOLD_S = 0.25;
    const MAX_FILE_MB      = 50;
    const ROOM_PREFIX      = 'mt-';
    const CODE_CHARS       = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
    const JOIN_MAX_ATTEMPTS = 4;
    const JOIN_TIMEOUT_MS   = 9000;
    const JOIN_RETRY_BASE_MS= 1200;

    // Multi-STUN: memperbesar peluang koneksi P2P tembus NAT.
    const PEER_OPTS = {
        debug: 0,
        config: {
            iceServers: [
                { urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] },
                { urls: 'stun:global.stun.twilio.com:3478' },
                { urls: 'stun:stun.cloudflare.com:3478' },
            ]
        }
    };

    // ─── STATE ─────────────────────────────────────────────
    const S = {
        isDJ:     false,
        roomCode: '',
        peer:     null,
        conns:    [],          // DJ→listener data channels
        calls:    new Map(),   // peerId → MediaConnection (active audio calls)
        audio:    null,
        audioStream: null,     // DJ's captured MediaStream
        audioCtx: null,
        queue:    [],          // [{name, url, size, file, duration}]
        songIdx:  -1,
        playing:  false,
        listeners: new Map(),  // peerId → {name, color}
        syncTimer: null,
        duration: 0,
        joinBusy:  false,      // joiner sedang mencoba koneksi
        joinAttempt: 0,
        joinTimer:   null,
        syncReqCount: 0,  // jumlah permintaan ulang stream saat join
    };

    // ─── DOM HELPERS ──────────────────────────────────────
    const $ = (s) => document.querySelector(s);
    const audio = () => S.audio;

    // ─── UTILITIES ────────────────────────────────────────
    function genCode() {
        let c = '';
        for (let i = 0; i < 6; i++) c += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)];
        return c;
    }
    function fmtTime(s) {
        if (!s || isNaN(s)) return '0:00';
        const m = Math.floor(s / 60), sec = Math.floor(s % 60);
        return m + ':' + (sec < 10 ? '0' : '') + sec;
    }
    function fmtSize(b) {
        if (b < 1024) return b + ' B';
        if (b < 1048576) return (b / 1024).toFixed(0) + ' KB';
        return (b / 1048576).toFixed(1) + ' MB';
    }
    function esc(t) { const d = document.createElement('span'); d.textContent = t; return d.innerHTML; }
    function rndColor() {
        const c = ['#e91e63','#9c27b0','#673ab7','#3f51b5','#2196f3','#00bcd4','#009688','#4caf50','#ff9800','#ff5722'];
        return c[Math.floor(Math.random() * c.length)];
    }
    function toast(msg) {
        const box = $('#toast-container');
        const el = document.createElement('div');
        el.className = 'toast';
        el.textContent = msg;
        box.appendChild(el);
        setTimeout(() => el.classList.add('out'), 3000);
        setTimeout(() => el.remove(), 3300);
    }
    function broadcast(data) {
        S.conns.forEach(c => { try { c.send(data); } catch (_) {} });
    }

    // ─── ROOM ──────────────────────────────────────────────
    function setJoinStatus(msg, cls) {
        const el = $('#join-status');
        if (!el) return;
        el.textContent = msg;
        el.className = cls ? 'join-status ' + cls : 'join-status';
    }

    function destroyPeer() {
        if (S.peer) { try { S.peer.destroy(); } catch (_) {} S.peer = null; }
    }

    function createRoom() {
        if (S.joinBusy) return;
        setJoinStatus('');
        destroyPeer();
        const code = genCode();
        const peer = new Peer(ROOM_PREFIX + code, PEER_OPTS);

        peer.on('open', () => {
            S.isDJ = true;
            S.roomCode = code;
            S.peer = peer;
            showRoom();
            toast('Ruangan dibuat — kode: ' + code);
        });

        peer.on('connection', onIncomingConn);
        peer.on('error', (err) => {
            console.error('DJ peer error:', err);
            if (err.type === 'unavailable-id') {
                peer.destroy();
                setTimeout(createRoom, 200);
                return;
            }
            toast('Error: ' + err.type);
        });
        peer.on('disconnected', () => {
            toast('⚠ Sinyal terputus — mencoba reconnect…');
        });
        peer.on('reconnected', () => {
            toast('Sinyal kembali normal.');
        });
        peer.on('close', () => {
            toast('Koneksi ke server sinyal hilang');
            goHome();
        });
    }

    // ── Listener join (with retry) ──────────────────────
    function joinRoom(code) {
        if (S.joinBusy) return;
        if (!code || code.length < 4) { toast('Masukkan kode ruangan yang valid'); return; }
        destroyPeer();
        S.joinBusy = true;
        S.joinAttempt = 0;
        setJoinStatus('Mencari ruangan…', 'connecting');
        $('#btn-join').disabled = true;
        $('#btn-create').disabled = true;

        function attempt() {
            S.joinAttempt++;
            const target = ROOM_PREFIX + code.toUpperCase();
            const peer = new Peer(undefined, PEER_OPTS);

            // Receive DJ's audio stream — set up BEFORE connect attempt
            peer.on('call', (call) => {
                call.answer();
                call.on('stream', (remoteStream) => {
                    S.syncReqCount = 0;
                    S.audio.srcObject = remoteStream;
                    S.audio.play().catch(() => {});
                });
                call.on('error', (e) => console.error('call err', e));
            });

            peer.on('open', () => {
                setJoinStatus('Bergabung… (percobaan ' + S.joinAttempt + ')', 'connecting');
                const conn = peer.connect(target, { reliable: true });

                const timer = setTimeout(() => {
                    // Timed out — destroy and retry
                    try { conn.close(); } catch (_) {}
                    peer.destroy();
                    retry();
                }, JOIN_TIMEOUT_MS);

                conn.on('open', () => {
                    clearTimeout(timer);
                    S.isDJ = false;
                    S.roomCode = code.toUpperCase();
                    S.peer = peer;
                    S.conns = [conn];
                    S.joinBusy = false;
                    $('#btn-join').disabled = false;
                    $('#btn-create').disabled = false;
                    setJoinStatus('');
                    showRoom();
                    toast('Berhasil gabung!');
                });

                conn.on('data', (d) => handleMsg(d, conn));
                conn.on('close', () => {
                    clearTimeout(timer);
                    toast('Koneksi terputus — sedang reconnect…');
                    S.joinBusy = true;
                    S.joinAttempt = 0;
                    peer.destroy();
                    retry();
                });
                conn.on('error', (e) => {
                    clearTimeout(timer);
                    console.error('conn error', e);
                    S.joinBusy = true;
                    S.joinAttempt = 0;
                    peer.destroy();
                    retry();
                });
            });

            peer.on('error', (err) => {
                console.error('join peer error:', err.type);
                peer.destroy();
                if (err.type === 'peer-unavailable') {
                    // Room truly not found — don't retry forever
                    if (S.joinAttempt >= 2) {
                        finishJoin('Ruangan tidak ditemukan. Pastikan DJ masih buka halaman.', 'error');
                        return;
                    }
                }
                retry();
            });
        }

        function retry() {
            if (S.joinAttempt >= JOIN_MAX_ATTEMPTS || !S.joinBusy) {
                finishJoin(S.joinBusy ? 'Gagal terhubung.' : '', 'error');
                return;
            }
            const delay = JOIN_RETRY_BASE_MS * S.joinAttempt;
            setJoinStatus('Mencoba ulang dalam ' + Math.round(delay / 1000) + 's…', 'retrying');
            S.joinTimer = setTimeout(attempt, delay);
        }

        function finishJoin(msg, cls) {
            S.joinBusy = false;
            $('#btn-join').disabled = false;
            $('#btn-create').disabled = false;
            setJoinStatus(msg, cls);
            if (msg) toast(msg);
            // If we were in room (reconnecting), go back home on final failure
            if (cls === 'error' && $('#room').classList.contains('active')) goHome();
        }

        attempt();
    }

    function leaveRoom() {
        if (S.syncTimer) { clearInterval(S.syncTimer); S.syncTimer = null; }
        if (S.joinTimer) { clearTimeout(S.joinTimer); S.joinTimer = null; }
        S.joinBusy = false;
        destroyPeer();
        S.conns = [];
        S.listeners.clear();
        S.queue.forEach(s => { if (s.url) URL.revokeObjectURL(s.url); });
        S.queue = [];
        S.songIdx = -1;
        S.playing = false;
        S.audioStream = null;
        if (S.audioCtx) { S.audioCtx.close(); S.audioCtx = null; }
        goHome();
    }

    // ─── DJ: INCOMING CONNECTION ───────────────────────────
    function onIncomingConn(conn) {
        conn.on('open', () => {
            S.conns.push(conn);

            const name = 'Pengguna ' + (1000 + Math.floor(Math.random() * 9000));
            S.listeners.set(conn.peer, { name, color: rndColor() });

            // Send full state to new listener
            conn.send({
                type: 'welcome',
                queue:   S.queue.map(s => ({ name: s.name, size: s.size, dur: s.duration })),
                songIdx: S.songIdx,
                playing: S.playing,
                time:    S.audio ? S.audio.currentTime : 0,
            });

            // Send audio stream
            sendStreamTo(conn);

            updateListeners();
            broadcast({ type: 'listener-join', name });
            toast(name + ' bergabung');
        });

        conn.on('data', (d) => handleMsg(d, conn));
        conn.on('close', () => {
            const info = S.listeners.get(conn.peer);
            S.conns = S.conns.filter(c => c.peer !== conn.peer);
            S.listeners.delete(conn.peer);
            updateListeners();
            if (info) { toast(info.name + ' keluar'); broadcast({ type: 'listener-leave', name: info.name }); }
        });
    }

    // ─── MESSAGE HANDLER ───────────────────────────────────
    function handleMsg(d, conn) {
        switch (d.type) {

            /* ── DJ receives ── */
            case 'request-sync': {
                if (!S.isDJ) break;
                conn.send({
                    type: 'welcome',
                    queue:   S.queue.map(s => ({ name: s.name, size: s.size, dur: s.duration })),
                    songIdx: S.songIdx,
                    playing: S.playing,
                    time:    S.audio ? S.audio.currentTime : 0,
                });
                sendStreamTo(conn);
                break;
            }

            /* ── Listener receives ── */
            case 'welcome': {
                if (S.isDJ) break;
                S.queue = (d.queue || []).map(q => ({ name: q.name, size: q.size, duration: q.dur, url: '' }));
                S.songIdx = d.songIdx ?? -1;
                if (S.songIdx >= 0 && S.queue[S.songIdx]?.duration) {
                    S.duration = S.queue[S.songIdx].duration;
                }
                updateQueue();
                if (S.songIdx >= 0 && S.queue[S.songIdx]) updateTrack(S.queue[S.songIdx].name);

                // Robustness: request audio stream if none arrived shortly after joining
                if (d.songIdx >= 0 && S.syncReqCount < 3) {
                    S.syncReqCount++;
                    setTimeout(() => {
                        if (!S.audio.srcObject && S.peer && S.peer.connected) {
                            try { S.conns.forEach(c => c.send({ type: 'request-sync' })); } catch (_) {}
                        }
                    }, 2500);
                }
                break;
            }

            case 'sync': {
                if (S.isDJ || !S.audio) break;
                const latency = (Date.now() - d.ts) / 1000;
                const target  = d.time + latency;
                /* Live MediaStream is inherently in sync (same stream via WebRTC,
                   jitter buffer only ~50-100ms). Seeking a stream would glitch it.
                   Only correct position when playing a local/blob file. */
                const isStream = !!S.audio.srcObject;
                if (!isStream && Math.abs(S.audio.currentTime - target) > SYNC_THRESHOLD_S) {
                    S.audio.currentTime = target;
                }
                if (d.playing && S.audio.paused)  S.audio.play().catch(() => {});
                if (!d.playing && !S.audio.paused) S.audio.pause();
                setPlayBtn(d.playing);
                if (S.duration && isFinite(S.duration)) updateProgress(target, S.duration);
                break;
            }

            case 'play':  { if (!S.isDJ && S.audio) { S.audio.play().catch(() => {}); setPlayBtn(true);  } break; }
            case 'pause': { if (!S.isDJ && S.audio) { S.audio.pause();                  setPlayBtn(false); } break; }

            case 'seek': {
                if (!S.isDJ && S.audio && d.time != null && !S.audio.srcObject) {
                    S.audio.currentTime = d.time;
                    updateProgress(d.time, S.audio.duration || 0);
                }
                break;
            }

            case 'song-change': {
                if (S.isDJ) break;
                S.songIdx = d.idx;
                if (S.queue[d.idx]) S.queue[d.idx].url = d.url || '';
                if (d.dur) S.duration = d.dur;
                updateTrack(d.name);
                updateQueue();
                setPlayBtn(true);
                break;
            }

            case 'queue-add': {
                if (S.isDJ) break;
                if (!S.queue.find(q => q.name === d.name)) {
                    S.queue.push({ name: d.name, size: d.size, duration: d.dur, url: '' });
                    updateQueue();
                }
                break;
            }

            case 'queue-remove': {
                if (S.isDJ) break;
                if (S.queue[d.idx]) { S.queue.splice(d.idx, 1); if (d.idx <= S.songIdx) S.songIdx--; updateQueue(); }
                break;
            }

            case 'listener-join':  { if (!S.isDJ) { S.listeners.set(d.name, { name: d.name, color: rndColor() }); updateListeners(); toast(d.name + ' bergabung'); } break; }
            case 'listener-leave': { if (!S.isDJ) { S.listeners.delete(d.name); updateListeners(); toast(d.name + ' keluar'); } break; }
        }
    }

    // ─── AUDIO STREAM (DJ) ────────────────────────────────
    function sendStreamTo(conn) {
        if (!S.audioStream) return;
        if (S.calls.has(conn.peer)) return;    // already streaming to this peer
        const call = S.peer.call(conn.peer, S.audioStream);
        call.on('error',  (e) => { console.error('call err', e); S.calls.delete(conn.peer); });
        call.on('close',  ()  => S.calls.delete(conn.peer));
        S.calls.set(conn.peer, call);
    }

    function ensureStreamToAll() {
        S.conns.forEach(conn => sendStreamTo(conn));
    }

    function ensureStream() {
        if (S.audioStream) return;
        S.audioCtx   = new (window.AudioContext || window.webkitAudioContext)();
        const source = S.audioCtx.createMediaElementSource(S.audio);
        const dest   = S.audioCtx.createMediaStreamDestination();
        source.connect(dest);
        source.connect(S.audioCtx.destination);   // DJ hears audio
        S.audioStream = dest.stream;
        ensureStreamToAll();                       // backfill streams to existing listeners
    }

    // ─── SYNC ENGINE (DJ) ────────────────────────────────
    function startSync() {
        if (S.syncTimer) clearInterval(S.syncTimer);
        S.syncTimer = setInterval(() => {
            if (S.isDJ && S.audio && S.conns.length > 0) {
                broadcast({
                    type: 'sync',
                    time: S.audio.currentTime,
                    playing: !S.audio.paused,
                    ts: Date.now(),
                });
            }
        }, SYNC_INTERVAL_MS);
    }

    // ─── PLAYER ───────────────────────────────────────────
    function playIdx(idx) {
        if (!S.isDJ || idx < 0 || idx >= S.queue.length) return;
        const song = S.queue[idx];
        if (!song.url) { toast('File tidak tersedia'); return; }

        ensureStream();
        S.songIdx = idx;
        S.audio.src = song.url;
        S.audio.play().catch(() => {});
        S.playing = true;

        updateTrack(song.name);
        updateQueue();
        setPlayBtn(true);
        S.duration = song.duration || 0;

        ensureStreamToAll();   // make sure every connected listener has the stream
        broadcast({ type: 'song-change', idx, name: song.name, dur: song.duration });
    }

    function nextSong() {
        if (!S.isDJ || S.queue.length === 0) return;
        playIdx((S.songIdx + 1) % S.queue.length);
    }
    function prevSong() {
        if (!S.isDJ || S.queue.length === 0) return;
        playIdx(S.songIdx <= 0 ? S.queue.length - 1 : S.songIdx - 1);
    }

    function togglePlay() {
        if (!S.isDJ || !S.audio) return;
        ensureStream();
        if (S.audio.paused) {
            S.audio.play().catch(() => {});
            setPlayBtn(true);
            broadcast({ type: 'play' });
        } else {
            S.audio.pause();
            setPlayBtn(false);
            broadcast({ type: 'pause' });
        }
    }

    function seekTo(pct) {
        if (!S.isDJ || !S.audio || !S.audio.duration) return;
        const t = pct * S.audio.duration;
        S.audio.currentTime = t;
        broadcast({ type: 'seek', time: t });
    }

    function removeSong(idx) {
        if (!S.isDJ || idx < 0 || idx >= S.queue.length) return;
        URL.revokeObjectURL(S.queue[idx].url);
        S.queue.splice(idx, 1);
        broadcast({ type: 'queue-remove', idx });

        if (S.queue.length === 0) {
            S.songIdx = -1; S.audio.src = ''; S.playing = false;
            updateTrack(null); updateQueue(); setPlayBtn(false);
        } else if (idx === S.songIdx) {
            playIdx(Math.min(idx, S.queue.length - 1));
        } else if (idx < S.songIdx) {
            S.songIdx--;
            updateQueue();
        } else {
            updateQueue();
        }
    }

    // ─── FILE UPLOAD (DJ) ─────────────────────────────────
    function handleFiles(files) {
        Array.from(files).forEach(f => {
            if (f.size > MAX_FILE_MB * 1048576) { toast(f.name + ' terlalu besar'); return; }
            const url = URL.createObjectURL(f);
            const name = f.name.replace(/\.[^/.]+$/, '');
            const song = { name, url, size: f.size, file: f, duration: 0 };

            const tmpAudio = new Audio(url);
            tmpAudio.addEventListener('loadedmetadata', () => {
                song.duration = tmpAudio.duration;
                updateQueue();
                broadcast({ type: 'queue-add', name: song.name, size: song.size, dur: song.duration });
            });

            S.queue.push(song);
            updateQueue();
            broadcast({ type: 'queue-add', name: song.name, size: song.size, dur: 0 });

            if (S.songIdx < 0) playIdx(S.queue.length - 1);
        });
    }

    // ─── UI UPDATES ──────────────────────────────────────
    function showRoom() {
        $('#home').classList.remove('active');
        $('#room').classList.add('active');
        $('#room-code').textContent = S.roomCode;
        if (S.isDJ) $('#dj-controls').classList.remove('hidden');
        // Listeners: freeze transport controls (DJ only)
        const locked = !S.isDJ;
        ['#btn-play', '#btn-next', '#btn-prev', '#btn-shuffle', '#btn-repeat'].forEach(sel => {
            $(sel).style.opacity = locked ? '0.35' : '1';
            $(sel).style.cursor = locked ? 'default' : 'pointer';
        });
        $('#progress-bar').style.cursor = locked ? 'default' : 'pointer';
        if (S.isDJ) startSync();
        updateQueue();
        updateListeners();
    }

    function goHome() {
        $('#home').classList.add('active');
        $('#room').classList.remove('active');
        $('#dj-controls').classList.add('hidden');
        $('#queue-list').innerHTML = '<li class="queue-empty">Belum ada lagu</li>';
        $('#listener-list').innerHTML = '';
        $('#track-title').textContent = 'Belum ada lagu';
        $('#track-artist').textContent = 'Upload musik untuk mulai';
        $('#album-art').classList.remove('playing');
        $('#progress-fill').style.width = '0%';
        $('#progress-thumb').style.left = '0%';
        $('#time-current').textContent = '0:00';
        $('#time-duration').textContent = '0:00';
        setPlayBtn(false);
        const el = $('#join-status'); if (el) el.textContent = '';
        $('#btn-join').disabled = false;
        $('#btn-create').disabled = false;
    }

    function updateTrack(name) {
        $('#track-title').textContent = name || 'Belum ada lagu';
        $('#track-artist').textContent = name ? (S.isDJ ? 'Sedang diputar' : 'DJ sedang memutar') : 'Upload musik untuk mulai';
        $('#album-art').classList.toggle('playing', !!name);
    }

    function setPlayBtn(on) {
        S.playing = on;
        $('#btn-play').textContent = on ? '⏸' : '▶';
        $('#album-art').classList.toggle('playing', on);
    }

    function updateProgress(time, dur) {
        if (!dur) return;
        const pct = (time / dur) * 100;
        $('#progress-fill').style.width = pct + '%';
        $('#progress-thumb').style.left = pct + '%';
        $('#time-current').textContent = fmtTime(time);
        $('#time-duration').textContent = fmtTime(dur);
    }

    function updateQueue() {
        const list = $('#queue-list');
        $('#queue-count').textContent = '(' + S.queue.length + ')';
        if (!S.queue.length) { list.innerHTML = '<li class="queue-empty">Belum ada lagu</li>'; return; }

        list.innerHTML = S.queue.map((s, i) => `
            <li class="queue-item ${i === S.songIdx ? 'active' : ''}" data-i="${i}">
                <span class="qi-index">${i === S.songIdx ? '▶' : i + 1}</span>
                <div class="qi-info">
                    <div class="qi-title">${esc(s.name)}</div>
                    <div class="qi-size">${fmtSize(s.size)}${s.duration ? ' · ' + fmtTime(s.duration) : ''}</div>
                </div>
                ${S.isDJ ? `<button class="qi-remove" data-i="${i}">✕</button>` : ''}
            </li>`).join('');

        list.querySelectorAll('.queue-item').forEach(el => {
            el.addEventListener('click', (e) => {
                if (e.target.closest('.qi-remove')) return;
                if (S.isDJ) playIdx(parseInt(el.dataset.i));
            });
        });
        list.querySelectorAll('.qi-remove').forEach(btn => {
            btn.addEventListener('click', (e) => { e.stopPropagation(); removeSong(parseInt(btn.dataset.i)); });
        });
    }

    function updateListeners() {
        const n = S.listeners.size + (S.isDJ ? 1 : 0);
        $('#listener-count').textContent = '👥 ' + n;
        let html = '';
        if (S.isDJ) html += `<li class="listener-item"><div class="listener-avatar" style="background:var(--accent)">DJ</div><span class="listener-name">Kamu (DJ)</span><span class="listener-tag">DJ</span></li>`;
        S.listeners.forEach(l => {
            html += `<li class="listener-item"><div class="listener-avatar" style="background:${l.color}">${l.name[0]}</div><span class="listener-name">${esc(l.name)}</span></li>`;
        });
        $('#listener-list').innerHTML = html;
    }

    // ─── INIT ─────────────────────────────────────────────
    document.addEventListener('DOMContentLoaded', () => {
        S.audio = $('#audio');
        S.audio.volume = 0.75;

        // ── Audio events (DJ) ──
        S.audio.addEventListener('timeupdate', () => {
            if (S.isDJ) updateProgress(S.audio.currentTime, S.audio.duration);
        });
        S.audio.addEventListener('loadedmetadata', () => {
            if (S.isDJ) { S.duration = S.audio.duration; updateProgress(0, S.audio.duration); }
        });
        S.audio.addEventListener('ended', () => { if (S.isDJ) nextSong(); });

        // ── Home buttons ──
        $('#btn-create').addEventListener('click', createRoom);
        $('#btn-join').addEventListener('click', () => {
            const code = $('#input-code').value.trim().toUpperCase();
            if (code.length < 4) { toast('Masukkan kode ruangan'); return; }
            joinRoom(code);
        });
        $('#input-code').addEventListener('keydown', (e) => { if (e.key === 'Enter') $('#btn-join').click(); });

        // ── Room controls ──
        $('#btn-leave').addEventListener('click', leaveRoom);
        $('#room-code').addEventListener('click', () => {
            navigator.clipboard.writeText(S.roomCode).then(() => toast('Kode disalin!')).catch(() => {});
        });

        // ── File upload ──
        $('#file-input').addEventListener('change', (e) => { handleFiles(e.target.files); e.target.value = ''; });
        const ua = $('#upload-area');
        ua.addEventListener('dragover',  (e) => { e.preventDefault(); ua.classList.add('dragover'); });
        ua.addEventListener('dragleave', ()  => ua.classList.remove('dragover'));
        ua.addEventListener('drop',      (e) => { e.preventDefault(); ua.classList.remove('dragover'); handleFiles(e.dataTransfer.files); });

        // ── Playback controls ──
        $('#btn-play').addEventListener('click', togglePlay);
        $('#btn-next').addEventListener('click', nextSong);
        $('#btn-prev').addEventListener('click', prevSong);

        // ── Progress bar seek ──
        let seeking = false;
        const pbar = $('#progress-bar');
        function seekFromEvent(e) {
            const r = pbar.getBoundingClientRect();
            seekTo(Math.max(0, Math.min(1, (e.clientX - r.left) / r.width)));
        }
        pbar.addEventListener('mousedown', (e) => { seeking = true; seekFromEvent(e); });
        document.addEventListener('mousemove', (e) => { if (seeking) seekFromEvent(e); });
        document.addEventListener('mouseup',   ()  => { seeking = false; });
        // Touch
        pbar.addEventListener('touchstart', (e) => { seeking = true; seekFromEvent(e.touches[0]); }, { passive: true });
        document.addEventListener('touchmove', (e) => { if (seeking) seekFromEvent(e.touches[0]); }, { passive: true });
        document.addEventListener('touchend',  ()  => { seeking = false; });

        // ── Volume ──
        $('#volume-slider').addEventListener('input', (e) => { S.audio.volume = e.target.value / 100; });

        // ── Placeholder buttons ──
        $('#btn-shuffle').addEventListener('click', () => toast('Shuffle: segera'));
        $('#btn-repeat').addEventListener('click',  () => toast('Repeat: segera'));
    });
})();