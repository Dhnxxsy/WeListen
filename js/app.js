/* ============================================================
   Music Together — P2P via MQTT signaling + WebRTC
   Tanpa VPS. Sinyal: broker MQTT publik (EMQX / fallback HiveMQ).
   Audio: WebRTC peer-to-peer (DJ → setiap pendengar).
   ============================================================ */

(() => {
    'use strict';

    // ─── CONFIG ────────────────────────────────────────────
    const BROKERS = [
        'wss://broker.emqx.io:8084/mqtt',
        'wss://broker.hivemq.com:8884/mqtt',
    ];
    const SYNC_INTERVAL_MS = 500;
    const BEAT_DJ_MS       = 4000;   // DJ heartbeat
    const BEAT_GUEST_MS    = 9000;   // guest heartbeat
    const GUEST_TIMEOUT_MS = 24000;  // DJ anggap guest keluar setelah ini
    const DJ_TIMEOUT_MS    = 15000;  // guest anggap DJ offline setelah ini
    const MAX_FILE_MB      = 50;
    const CODE_CHARS       = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

    const RTC_OPTS = {
        iceCandidatePoolSize: 4,
        iceServers: [
            { urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] },
            { urls: 'stun:global.stun.twilio.com:3478' },
            { urls: 'stun:stun.cloudflare.com:3478' },
        ]
    };

    // ─── STATE ─────────────────────────────────────────────
    const S = {
        isDJ:     false,
        roomCode: '',
        mq:       null,          // mqtt client
        topic:    '',

        // DJ
        djStream: null,
        djTrack:  null,
        djCtx:    null,
        djAnalyser: null,
        djRms:    0,
        rmsT:     null,
        pcs:      new Map(),     // guestId → RTCPeerConnection
        guests:   new Map(),     // guestId → {name, lastSeen}
        beatT:    null,
        syncT:    null,
        screenStream: null,
        screenActive: false,

        // Guest
        guestId:  'g-' + Math.random().toString(36).slice(2, 8),
        others:   new Map(),  // guest-side roster (id → name), exclude self
        pc:       null,
        gotStream:false,
        gotVideo: false,
        announced:false,
        djAlive:  0,            // last time DJ seen (ms)
        beatGT:   null,
        pubT:     null,   // DJ broadcast state periodik
        rmsWarned: false,

        audio:    null,
        queue:    [],   // [{name, url, size, dur}]
        songIdx:  -1,
        playing:  false,
        duration: 0,

        joinBusy: false,
    };

    // ─── DOM ───────────────────────────────────────────────
    const $ = (s) => document.querySelector(s);

    // ─── UTILITIES ─────────────────────────────────────────
    function genCode() {
        let c = '';
        for (let i = 0; i < 6; i++) c += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)];
        return c;
    }
    function esc(t) { const d = document.createElement('span'); d.textContent = t; return d.innerHTML; }
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
    function rndColor() {
        const c = ['#e91e63','#9c27b0','#673ab7','#3f51b5','#2196f3','#00bcd4','#009688','#4caf50','#ff9800','#ff5722'];
        return c[Math.floor(Math.random() * c.length)];
    }
    // HD Opus: 512kbps stereo full-band via SDP munging
    function mungeHdSdp(sdp) {
        if (sdp && typeof sdp === 'object' && typeof sdp.sdp === 'string') sdp = sdp.sdp;
        if (typeof sdp !== 'string') return sdp || '';
        const m = sdp.match(/a=rtpmap:(\d+) opus\/48000\/2/);
        if (!m) return sdp;
        const pt = m[1];
        return sdp.replace(
            new RegExp('a=fmtp:' + pt + ' ([^\r\n]*)'),
            (all, params) => {
                const hd = ['maxaveragebitrate=512000','maxplaybackrate=48000','stereo=1','sprop-stereo=1'];
                let p = params;
                for (const h of hd) if (!p.includes(h.split('=')[0])) p += ';' + h;
                return 'a=fmtp:' + pt + ' ' + p;
            }
        );
    }
    function hintMusic(track) { try { if (track) track.contentHint = 'music'; } catch (_) {} }
    function toast(msg) {
        const box = $('#toast-container');
        const el = document.createElement('div');
        el.className = 'toast';
        el.textContent = msg;
        box.appendChild(el);
        setTimeout(() => el.classList.add('out'), 3000);
        setTimeout(() => el.remove(), 3300);
    }
    function dec(payload) {
        if (typeof payload === 'string') return JSON.parse(payload);
        if (payload instanceof ArrayBuffer) payload = new Uint8Array(payload);
        return JSON.parse(new TextDecoder().decode(payload));
    }
    function setJoinStatus(msg, cls) {
        const el = $('#join-status');
        if (!el) return;
        el.textContent = msg || '';
        el.className = 'join-status' + (cls ? ' ' + cls : '');
    }

    // ─── MQTT (broker manager) ─────────────────────────────
    function connectBroker(onReady, onMsg) {
        const guarded = (d) => { if (d && d.src === selfSrc()) return; onMsg(d); };
        let idx = 0;
        function tryNext() {
            const url = BROKERS[idx];
            setJoinStatus('Menghubungkan server… ' + (idx + 1) + '/' + BROKERS.length, 'connecting');
            const client = mqtt.connect(url, {
                clientId:   S.isDJ ? 'dj-' + Math.random().toString(36).slice(2, 10)
                                   : 'g-' + Math.random().toString(36).slice(2, 10),
                clean:      true,
                reconnectPeriod: 1000,
                connectTimeout: 8000,
            });
            client.on('connect', () => {
                S.mq = client;
                client.subscribe(S.topic, { qos: 0 });
                onReady();
            });
            client.on('message', (t, p) => { try { guarded(dec(p)); } catch (e) {} });
            client.on('offline', () => { /* auto-reconnect */ });
            client.on('close', () => {
                if (!S.mq && idx < BROKERS.length - 1) {
                    idx++;
                    setTimeout(tryNext, 500);
                }
            });
            client.on('error', () => { /* handled via close/offline */ });
        }
        tryNext();
    }
    function pub(obj) {
        if (!S.mq) return;
        obj.src = S.isDJ ? 'dj' : S.guestId;   // self-echo filter
        S.mq.publish(S.topic, JSON.stringify(obj));
    }
    function selfSrc() { return S.isDJ ? 'dj' : S.guestId; }

    // ─── STREAM AUDIO DJ ───────────────────────────────────
    function ensureStream() {
        if (S.djStream) return;
        try {
            if (!S.djCtx) S.djCtx = new (window.AudioContext || window.webkitAudioContext)();
            const src = S.djCtx.createMediaElementSource(S.audio);
            const dest = S.djCtx.createMediaStreamDestination();
            src.connect(dest);
            src.connect(S.djCtx.destination);   // DJ dengar lagu
            S.djAnalyser = S.djCtx.createAnalyser();
            S.djAnalyser.fftSize = 1024;
            src.connect(S.djAnalyser);          // self-test: ukur RMS capture
            S.djStream = dest.stream;
            S.djTrack = dest.stream.getAudioTracks()[0];
            hintMusic(S.djTrack);
            console.log('[mtm] DJ capture dibuat, track=', !!S.djTrack, 'ctx=', S.djCtx.state, 'rmsPath=on');
            // Renegosiasi untuk guest yang terlanjur dapet answer tanpa track
            if (S.djTrack) {
                S.pcs.forEach((_pc, gid) => { if (_pc.getSenders().length === 0) reansGuest(gid); });
            }
        } catch (e) {
            console.error('ensureStream error', e);
        }
    }
    function djRms() {
        if (!S.djAnalyser) return 0;
        const n = S.djAnalyser.fftSize;
        const buf = new Float32Array(n);
        S.djAnalyser.getFloatTimeDomainData(buf);
        let sum = 0;
        for (let i = 0; i < n; i++) sum += buf[i] * buf[i];
        return Math.sqrt(sum / n);
    }
    // Renegosiasi: attach track ke guest yang jawabannya tidak punya audio
    async function reansGuest(guestId) {
        const pc = S.pcs.get(guestId);
        if (!pc || !S.djTrack || pc.getSenders().length > 0) return;
        pc.addTrack(S.djTrack, S.djStream);
        console.log('[mtm] renegosiasi track untuk guest', guestId);
        try {
            const offer = mungeHdSdp((await pc.createOffer()).sdp);
            await pc.setLocalDescription({ type: 'offer', sdp: offer });
            pub({ type: 'reans', to: guestId, sdp: pc.localDescription });
        } catch (e) { console.error('reansGuest error', e); }
    }

    // ─── WEBRTC ────────────────────────────────────────────
    // Tentukan track apa yang dikirim: screen (video+audio sistem) atau audio lagu biasa
    function attachDjTracks(pc) {
        if (S.screenActive && S.screenStream) {
            const vid = S.screenStream.getVideoTracks()[0];
            const aud = S.screenStream.getAudioTracks()[0];
            if (aud) hintMusic(aud);
            if (vid) { pc.addTrack(vid, S.screenStream); console.log('[mtm] SCREEN video ke pc'); }
            if (aud) { pc.addTrack(aud, S.screenStream); console.log('[mtm] SCREEN audio (sistem) ke pc — musik lewat layar'); }
            else if (S.djTrack) { pc.addTrack(S.djTrack, S.djStream); console.log('[mtm] layar TANPA audio → kirim audio player'); }
            return vid !== undefined || aud !== undefined || S.djTrack !== undefined;
        }
        if (S.djTrack) {
            pc.addTrack(S.djTrack, S.djStream);
            console.log('[mtm] DJ attach track ke guest');
            return true;
        }
        console.warn('[mtm] DJ belum punya track saat answer guest');
        return false;
    }
    // DJ side: answer guest's recvonly offer
    async function answerGuest(guestId, offerSdp) {
        closePc(guestId);
        if (!S.djTrack) ensureStream();
        const pc = new RTCPeerConnection(RTC_OPTS);
        S.pcs.set(guestId, pc);
        attachDjTracks(pc);
        pc.onicecandidate = (e) => { if (e.candidate) pub({ type: 'ice', to: guestId, candidate: e.candidate }); };
        pc.onconnectionstatechange = () => {
            if (pc.connectionState === 'failed' || pc.connectionState === 'closed') {
                closePc(guestId);
            }
            connMon();
        };
        try {
            await pc.setRemoteDescription(offerSdp);
            (pc._pending || []).forEach(c => { try { pc.addIceCandidate(c); } catch (_) {} });
            pc._pending = [];
            const ans = mungeHdSdp((await pc.createAnswer()).sdp);
            await pc.setLocalDescription({ type: 'answer', sdp: ans });
            pub({ type: 'answer', to: guestId, sdp: pc.localDescription });
        } catch (e) {
            console.error('answerGuest error', e);
            closePc(guestId);
        }
    }
    // Guest yang sudah terhubung di-close → mereka re-offer & dapat layout baru
    function bounceAllGuests() {
        S.pcs.forEach((_pc, gid) => closePc(gid));
    }
    function closePc(guestId) {
        const pc = S.pcs.get(guestId);
        if (pc) { try { pc.close(); } catch (_) {} S.pcs.delete(guestId); }
    }
    function addIce(guestId, cand) {
        const pc = S.pcs.get(guestId);
        if (!pc || !cand) return;
        if (pc.remoteDescription) { try { pc.addIceCandidate(cand); } catch (_) {} }
        else { (pc._pending = pc._pending || []).push(cand); }   // buffer kandidat dini (Firefox)
    }
    function setConn(msg, cls) {
        const el = $('#conn-status');
        if (!el) return;
        el.textContent = msg || '—';
        el.className = 'conn-status' + (cls ? ' ' + cls : '');
    }
    function connMon() {
        if (S.isDJ) {
            const total = S.pcs.size;
            const ok = Array.from(S.pcs.values()).filter(p => p.connectionState === 'connected').length;
            if (!total) setConn('Menunggu pendengar…');
            else setConn(ok === total ? '👥 ' + total + ' terhubung' : '⏳ ' + ok + '/' + total + ' terhubung', ok === total ? 'ok' : 'warn');
        } else {
            const st = S.pc ? S.pc.connectionState : 'new';
            switch (st) {
                case 'connected': setConn('🔊 Terhubung ke DJ', 'ok'); break;
                case 'failed':    setConn('⚠️ Koneksi gagal — mencoba ulang…', 'bad'); break;
                case 'closed':    setConn('Koneksi ditutup', 'warn'); break;
                default:          setConn('Menghubungkan…'); break;
            }
        }
    }

    // Guest side: recvonly audio+video, trickle ICE, apply answer
    async function guestOffer() {
        if (S.isDJ || S.pc) return;
        setConn('Menghubungkan…');
        const pc = new RTCPeerConnection(RTC_OPTS);
        pc.addTransceiver('audio', { direction: 'recvonly' });
        pc.addTransceiver('video', { direction: 'recvonly' });
        pc.onicecandidate = (e) => { if (e.candidate) pub({ type: 'ice', id: S.guestId, candidate: e.candidate }); };
        pc.ontrack = (ev) => {
            if (ev.track.kind === 'video') {
                S.gotVideo = true;
                const v = $('#screen-video');
                if (v) v.srcObject = new MediaStream([ev.track]);
                const p = $('#screen-panel'); if (p) p.classList.remove('hidden');
                console.log('[mtm] guest terima VIDEO track');
                return;
            }
            S.gotStream = true;
            if (pc._watch) { clearTimeout(pc._watch); pc._watch = null; }
            S.audio.srcObject = new MediaStream([ev.track]);
            S.audio.muted = false;
            hideTapToHear();
            console.log('[mtm] guest terima track audio, peerState=', pc.connectionState);
            tryAutoPlay();
        };
        pc.onconnectionstatechange = () => {
            connMon();
            if (pc.connectionState === 'connected') {
                setJoinStatus('');
                if (!S.announced) { S.announced = true; toast('Berhasil gabung!'); }
            }
            if (pc.connectionState === 'failed') {
                if (pc._watch) { clearTimeout(pc._watch); pc._watch = null; }
                closeGuestPc();
                S.gotStream = false;
                toast('Koneksi audio bermasalah — mencoba ulang…');
            }
        };
        S.pc = pc;
        try {
            const offer = mungeHdSdp((await pc.createOffer()).sdp);
            await pc.setLocalDescription({ type: 'offer', sdp: offer });
            pub({ type: 'offer', id: S.guestId, sdp: pc.localDescription });
        } catch (e) {
            console.error('guestOffer error', e);
            closeGuestPc();
        }
        // Watchdog: connected tapi belum ada track → coba ulang sekali
        pc._watch = setTimeout(() => {
            if (pc.connectionState === 'connected' && !S.gotStream && !S.gotVideo) {
                console.warn('[mtm] connected tanpa media — re-offer ulang');
                closeGuestPc();
                guestOffer();
            }
        }, 6000);
    }
    function hideGuestScreen() {
        const v = $('#screen-video'); if (v) v.srcObject = null;
        const p = $('#screen-panel'); if (p) p.classList.add('hidden');
    }

    // ─── SHARE SCREEN (DJ) ─────────────────────────────────
    async function startScreenShare() {
        if (!S.isDJ || S.screenActive) return;
        if (!navigator.mediaDevices || !navigator.mediaDevices.getDisplayMedia) {
            toast('Browser tidak mendukung share screen');
            return;
        }
        let stream;
        try {
            stream = await navigator.mediaDevices.getDisplayMedia({
                video: { frameRate: { ideal: 24, max: 30 } },
                audio: {
                    suppressLocalAudioPlayback: true,
                    echoCancellation: false,
                    noiseSuppression: false,
                    autoGainControl: false
                },
                systemAudio: 'exclude',
                selfBrowserSurface: 'exclude',
                surfaceSwitching: 'include'
            });
        } catch (e) {
            console.warn('[mtm] getDisplayMedia dibatalkan/tidak diizinkan', e);
            toast('Share screen dibatalkan');
            return;
        }
        if (!stream.getVideoTracks()[0]) { toast('Tidak ada video dari pilihan layar'); try { stream.getTracks().forEach(t => t.stop()); } catch (_) {} return; }
        S.screenStream = stream;
        S.screenActive = true;
        const a = stream.getAudioTracks()[0];
        if (a) hintMusic(a);
        // Preview untuk DJ (muted — DJ dengar audio asli dari speaker sendiri)
        const v = $('#screen-video');
        if (v) { v.srcObject = stream; }
        const panel = $('#screen-panel'); if (panel) panel.classList.remove('hidden');
        $('#btn-stop-screen').hidden = false;
        $('#btn-share-screen').classList.add('active');
        // Hentikan saat user stop lewat UI browser
        const vt = stream.getVideoTracks()[0];
        if (vt) vt.onended = () => { if (S.screenActive) stopScreenShare(); };
        pub({ type: 'screen', active: true });
        console.log('[mtm] screen share ON, audio=', !!a);
        toast(a ? '🖥️ Layar + audio sistem sedang dibagikan' : '🖥️ Layar dibagikan (audio tetap dari player)');
        // Guest harus re-konek untuk dapat layout baru
        bounceAllGuests();
    }
    function stopScreenShare() {
        if (S.screenStream) { try { S.screenStream.getTracks().forEach(t => t.stop()); } catch (_) {} S.screenStream = null; }
        S.screenActive = false;
        hideScreenUi();
        pub({ type: 'screen', active: false });
        console.log('[mtm] screen share OFF');
        toast('🖥️ Share screen dihentikan');
        bounceAllGuests();
    }
    function toggleScreenShare() {
        if (S.screenActive) stopScreenShare();
        else startScreenShare();
    }
    function hideScreenUi() {
        const v = $('#screen-video'); if (v) v.srcObject = null;
        const panel = $('#screen-panel'); if (panel) panel.classList.add('hidden');
        const stop = $('#btn-stop-screen'); if (stop) stop.hidden = true;
        const btn = $('#btn-share-screen'); if (btn) btn.classList.remove('active');
    }
    function closeGuestPc() {
        if (S.pc) {
            if (S.pc._watch) { clearTimeout(S.pc._watch); S.pc._watch = null; }
            try { S.pc.close(); } catch (_) {}
            S.pc = null;
        }
        S.gotStream = false;
        S.gotVideo = false;
    }
    // Autoplay bisa diblokir Chrome (tanpa gesture) → tampilkan tombol sekali klik
    function tryAutoPlay() {
        if (!S.audio) return;
        const p = S.audio.play();
        if (p && p.catch) {
            p.then(() => { if (!S.audio.paused) hideTapToHear(); }).catch((e) => {
                console.warn('[mtm] autoplay terblokir:', e && e.name);
                const btn = $('#tap-to-hear');
                if (btn) btn.hidden = false;
                toast('🔊 Ketuk tombol untuk mendengar audio');
            });
        }
    }
    function hideTapToHear() { const b = $('#tap-to-hear'); if (b) b.hidden = true; }

    // ─── ROOM — DJ ─────────────────────────────────────────
    function createRoom() {
        if (S.joinBusy) return;
        S.isDJ = true;
        S.roomCode = genCode();
        S.topic = 'mtm/' + S.roomCode;
        S.joinBusy = true;
        setJoinStatus('Membuat ruangan…', 'connecting');
        destroyBroker();
        // AudioContext dibikin dalam gesture klik → tidak diblokir autoplay policy
        try { if (!S.djCtx) S.djCtx = new (window.AudioContext || window.webkitAudioContext)(); } catch (_) {}

        connectBroker(() => {
            S.joinBusy = false;
            setJoinStatus('');
            ensureStream();
            showRoom();
            toast('Ruangan dibuat — kode: ' + S.roomCode);

            // Heartbeat DJ + roster sweep
            S.beatT = setInterval(() => {
                pub({ type: 'beat' });
                const now = Date.now();
                S.guests.forEach((g, id) => {
                    if (now - g.lastSeen > GUEST_TIMEOUT_MS) {
                        S.guests.delete(id);
                        closePc(id);
                        pub({ type: 'l', id, name: g.name });
                        updateListenersL();
                    }
                });
            }, BEAT_DJ_MS);

            // Broadcast state periodik (refresh utk yg telat gabung / resync)
            S.pubT = setInterval(() => { pubState(); }, 5000);

            // Pemantau capture DJ: DJ dengar = capture keluar? (RMS live stream)
            let rmsSilent = 0;
            S.rmsT = setInterval(() => {
                if (!S.isDJ || !S.audio || S.audio.paused) { rmsSilent = 0; return; }
                S.djRms = djRms();
                if (S.djRms < 0.001) {
                    if (++rmsSilent >= 3 && S.queue.length) {
                        console.warn('[mtm] CAPTURE SENYAP meski element diputar (rms=', S.djRms.toFixed(5), ')');
                        S.rmsWarned = true;
                        toast('⚠️ Host: audio peserta terputus — refresh halaman ini');
                        rmsSilent = 0;
                    }
                } else { rmsSilent = 0; }
            }, 1000);
        }, onDjMsg);
    }

    function onDjMsg(d) {
        switch (d.type) {
            case 'hello': {
                S.guests.set(d.id, { name: d.name, lastSeen: Date.now() });
                updateListenersL();
                pub({ type: 'j', id: d.id, name: d.name });
                pubState();
                break;
            }
            case 'offer':
                S.guests.set(d.id, { name: S.guests.get(d.id)?.name || d.name || 'Pengguna', lastSeen: Date.now() });
                answerGuest(d.id, d.sdp);
                break;
            case 'ice':
                addIce(d.id, d.candidate);
                break;
            case 'answer': {
                const pc = S.pcs.get(d.id);
                if (pc && pc.signalingState !== 'stable') {
                    pc.setRemoteDescription(d.sdp).catch((e) => console.error('dj setRemote err', e));
                }
                break;
            }
            case 'beatG': {
                const g = S.guests.get(d.id);
                if (g) { g.lastSeen = Date.now(); }
                else {
                    S.guests.set(d.id, { name: d.name, lastSeen: Date.now() });
                    pub({ type: 'j', id: d.id, name: d.name });
                    updateListenersL();
                }
                break;
            }
            case 'reqstate': pubState(); break;
            case 'bye': {
                S.guests.delete(d.id);
                closePc(d.id);
                pub({ type: 'l', id: d.id, name: d.name });
                updateListenersL();
                break;
            }
        }
    }

    function pubState() {
        pub({
            type: 'state',
            queue: S.queue.map(q => ({ name: q.name, size: q.size, dur: q.dur })),
            song: S.songIdx,
            playing: S.playing,
            time: S.audio ? S.audio.currentTime : 0,
            dur: S.duration,
            dj: true,
            screen: S.screenActive,
            guests: Array.from(S.guests).map(([id, g]) => ({ id, name: g.name })),
        });
    }

    // ─── ROOM — GUEST ──────────────────────────────────────
    function joinRoom(code) {
        if (S.joinBusy) return;
        if (!code || code.length < 4) { toast('Masukkan kode ruangan yang valid'); return; }
        S.isDJ = false;
        S.roomCode = code.toUpperCase();
        S.topic = 'mtm/' + S.roomCode;
        S.joinBusy = true;
        destroyBroker();
        $('#btn-join').disabled = true;
        $('#btn-create').disabled = true;

        connectBroker(() => {
            // Hello + offer; re-offer sampai dapat answer
            pub({ type: 'hello', id: S.guestId, name: 'Pengguna' });
            setTimeout(() => guestOffer(), 300);
            const retryOffer = setInterval(() => {
                if (!S.gotStream) guestOffer();
            }, 6000);
            S.beatGT = setInterval(() => {
                pub({ type: 'beatG', id: S.guestId, name: 'Pengguna' });
            }, BEAT_GUEST_MS);

            // DJ presence watchdog
            setInterval(() => {
                const check = Date.now() - S.djAlive;
                if (check > DJ_TIMEOUT_MS) {
                    setJoinStatus('Menunggu DJ online…', 'retrying');
                }
            }, 3000);

            // Stop refresh loops when room closes
            S._stopGuestLoops = () => {
                clearInterval(retryOffer);
                clearInterval(S.beatGT);
            };
        }, onGuestMsg);

        // watchdog untuk broker connect timeout
        setTimeout(() => {
            if (S.joinBusy && !S.roomShown) setJoinStatus('Menunggu DJ online…', 'retrying');
        }, 6000);
    }

    function onGuestMsg(d) {
        S.djAlive = Date.now();
        switch (d.type) {
            case 'beat': S.djAlive = Date.now(); break;
            case 'state': {
                S.queue = (d.queue || []).map(q => ({ name: q.name, size: q.size, dur: q.dur, url: '' }));
                S.duration = d.dur || 0;
                S.songIdx = d.song ?? -1;
                S.others = new Map((d.guests || []).filter(g => g.id !== S.guestId).map(g => [g.id, g.name]));
                updateQueue();
                updateListenersL();
                if (S.songIdx >= 0 && S.queue[S.songIdx]) updateTrack(S.queue[S.songIdx].name);
                if (d.playing && !S.gotStream && !S.pc) setTimeout(() => guestOffer(), 400);
                showRoom();
                S.roomShown = true;
                break;
            }
            case 'song':
                S.songIdx = d.idx;
                if (S.queue[d.idx]) S.queue[d.idx].url = d.url || '';
                if (d.dur) S.duration = d.dur;
                updateTrack(d.name);
                updateQueue();
                setPlayBtn(true);
                break;
            case 'qadd':
                if (!S.queue.find(q => q.name === d.name)) { S.queue.push({ name: d.name, size: d.size, dur: d.dur, url: '' }); updateQueue(); }
                break;
            case 'qrm':
                if (S.queue[d.idx]) { S.queue.splice(d.idx, 1); if (d.idx <= S.songIdx) S.songIdx--; updateQueue(); }
                break;
            case 'play':
                if (S.audio) { hideTapToHear(); tryAutoPlay(); setPlayBtn(true); } break;
            case 'pause': if (S.audio) { S.audio.pause(); hideTapToHear(); setPlayBtn(false); } break;
            case 'seek':  if (S.audio && !S.audio.srcObject && d.time != null) { S.audio.currentTime = d.time; } break;
            case 'screen': {
                if (!d.active) hideGuestScreen();
                closeGuestPc();
                S.gotStream = false;
                setTimeout(() => guestOffer(), 900);
                break;
            }
            case 'syn': {
                if (!S.audio) break;
                const lat = (Date.now() - d.ts) / 1000;
                const target = d.time + lat;
                if (!S.audio.srcObject && Math.abs(S.audio.currentTime - target) > 0.25) {
                    S.audio.currentTime = target;
                }
                if (d.playing && S.audio.paused) tryAutoPlay();
                if (!d.playing && !S.audio.paused) { S.audio.pause(); hideTapToHear(); }
                setPlayBtn(d.playing);
                if (S.duration && isFinite(S.duration)) updateProgress(target, S.duration);
                break;
            }
            case 'answer':
                if (d.to !== S.guestId) break;   // jawaban untuk guest lain
                if (S.pc && S.pc.localDescription && S.pc.signalingState === 'have-local-offer') {
                    S.pc.setRemoteDescription(d.sdp)
                        .then(() => { (S.pc._pending || []).forEach(c => { try { S.pc.addIceCandidate(c); } catch (_) {} }); S.pc._pending = []; })
                        .catch((e) => { console.error('setRemote err', e); closeGuestPc(); S.gotStream = false; setTimeout(() => guestOffer(), 700); });
                }
                break;
            case 'reans':
                if (d.to !== S.guestId) break;
                if (S.pc && d.sdp) {
                    S.pc.setRemoteDescription(d.sdp)
                        .then(async () => {
                            const ans = await S.pc.createAnswer();
                            await S.pc.setLocalDescription(ans);
                            pub({ type: 'answer', id: S.guestId, sdp: S.pc.localDescription });
                            console.log('[mtm] chat renegosiasi selesai (DJ kirim ulang track)');
                        })
                        .catch((e) => console.error('reans err', e));
                }
                break;
            case 'ice':
                if (d.to !== S.guestId) break;
                if (S.pc && d.candidate) {
                    if (S.pc.remoteDescription) { try { S.pc.addIceCandidate(d.candidate); } catch (_) {} }
                    else { (S.pc._pending = S.pc._pending || []).push(d.candidate); }
                }
                break;
            case 'j':
                if (d.id !== S.guestId) S.others.set(d.id, d.name);
                updateListenersL();
                break;
            case 'l':
                S.others.delete(d.id);
                updateListenersL();
                break;
        }
    }

    // ─── TRANSPORT (DJ) ────────────────────────────────────
    function togglePlay() {
        if (!S.isDJ || !S.audio) return;
        ensureStream();
        try { if (S.djCtx && S.djCtx.state === 'suspended') S.djCtx.resume(); } catch (_) {}
        if (S.audio.paused) {
            S.audio.play().catch(() => {});
            S.playing = true;
            setPlayBtn(true);
            pub({ type: 'play' });
        } else {
            S.audio.pause();
            S.playing = false;
            setPlayBtn(false);
            pub({ type: 'pause' });
        }
    }
    function playIdx(idx) {
        if (!S.isDJ || idx < 0 || idx >= S.queue.length) return;
        const song = S.queue[idx];
        if (!song.url) return;
        ensureStream();
        try { if (S.djCtx && S.djCtx.state === 'suspended') S.djCtx.resume(); } catch (_) {}
        S.songIdx = idx;
        S.audio.src = song.url;
        S.audio.play().catch(() => {});
        S.playing = true;
        S.duration = song.dur || 0;
        updateTrack(song.name);
        updateQueue();
        setPlayBtn(true);
        pub({ type: 'song', idx, name: song.name, dur: song.dur });
    }
    function nextSong() { if (S.isDJ && S.queue.length) playIdx((S.songIdx + 1) % S.queue.length); }
    function prevSong() { if (S.isDJ && S.queue.length) playIdx(S.songIdx <= 0 ? S.queue.length - 1 : S.songIdx - 1); }
    function seekTo(pct) {
        if (!S.isDJ || !S.audio || !S.audio.duration) return;
        const t = pct * S.audio.duration;
        S.audio.currentTime = t;
        pub({ type: 'seek', time: t });
    }
    function removeSong(idx) {
        if (!S.isDJ || idx < 0 || idx >= S.queue.length) return;
        URL.revokeObjectURL(S.queue[idx].url);
        S.queue.splice(idx, 1);
        pub({ type: 'qrm', idx });
        if (!S.queue.length) {
            S.songIdx = -1; S.audio.src = ''; S.playing = false;
            updateTrack(null); updateQueue(); setPlayBtn(false);
        } else if (idx === S.songIdx) {
            playIdx(Math.min(idx, S.queue.length - 1));
        } else {
            if (idx < S.songIdx) S.songIdx--;
            updateQueue();
        }
    }

    // ─── FILE UPLOAD (DJ) ──────────────────────────────────
    function handleFiles(files) {
        Array.from(files).forEach(f => {
            if (f.size > MAX_FILE_MB * 1048576) { toast(f.name + ' terlalu besar'); return; }
            const url = URL.createObjectURL(f);
            const name = f.name.replace(/\.[^/.]+$/, '');
            const song = { name, url, size: f.size, dur: 0 };
            const t = new Audio(url);
            t.addEventListener('loadedmetadata', () => {
                song.dur = t.duration;
                updateQueue();
                pub({ type: 'qadd', name: song.name, size: song.size, dur: song.dur });
            });
            S.queue.push(song);
            updateQueue();
            pub({ type: 'qadd', name: song.name, size: song.size, dur: 0 });
            if (S.songIdx < 0) playIdx(S.queue.length - 1);
        });
    }

    // ─── SYNC ENGINE (DJ) ──────────────────────────────────
    function startSync() {
        if (S.syncT) clearInterval(S.syncT);
        S.syncT = setInterval(() => {
            if (S.isDJ && S.audio) {
                pub({ type: 'syn', time: S.audio.currentTime, playing: !S.audio.paused, ts: Date.now() });
            }
        }, SYNC_INTERVAL_MS);
    }

    // ─── LEAVE / DESTROY ───────────────────────────────────
    function leaveRoom() {
        if (S.isDJ) pub({ type: 'bye' });
        else {
            pub({ type: 'bye', id: S.guestId });
            if (S._stopGuestLoops) S._stopGuestLoops();
        }
        destroyAll();
        goHome();
    }
    function destroyBroker() {
        if (S.mq) { try { S.mq.end(true); } catch (_) {} S.mq = null; }
        if (S.beatT) { clearInterval(S.beatT); S.beatT = null; }
        if (S.syncT) { clearInterval(S.syncT); S.syncT = null; }
        if (S.pubT)  { clearInterval(S.pubT);  S.pubT  = null; }
        if (S.rmsT)  { clearInterval(S.rmsT);  S.rmsT  = null; }
        S.djRms = 0;
        if (S.beatGT) { clearInterval(S.beatGT); S.beatGT = null; }
        if (S._stopGuestLoops) { S._stopGuestLoops(); S._stopGuestLoops = null; }
    }
    function destroyAll() {
        destroyBroker();
        if (S.screenStream) { try { S.screenStream.getTracks().forEach(t => t.stop()); } catch (_) {} S.screenStream = null; }
        S.screenActive = false;
        hideScreenUi();
        hideGuestScreen();
        S.pcs.forEach((pc, id) => closePc(id));
        S.guests.clear();
        if (S.pc) closeGuestPc();
        S.queue.forEach(s => { if (s.url) URL.revokeObjectURL(s.url); });
        S.queue = [];
        S.songIdx = -1;
        S.playing = false;
        S.duration = 0;
        S.gotStream = false;
        S.gotVideo = false;
        S.announced = false;
        S.others.clear();
        S.roomShown = false;
        S.djRms = 0;
        S.rmsWarned = false;
    }

    // ─── UI ────────────────────────────────────────────────
    function showRoom() {
        $('#home').classList.remove('active');
        $('#room').classList.add('active');
        $('#room-code').textContent = S.roomCode;
        $('#btn-join').disabled = false;
        $('#btn-create').disabled = false;
        S.joinBusy = false;
        setJoinStatus('');
        if (S.isDJ) $('#dj-controls').classList.remove('hidden');
        else $('#dj-controls').classList.add('hidden');
        const locked = !S.isDJ;
        ['#btn-play', '#btn-next', '#btn-prev', '#btn-shuffle', '#btn-repeat'].forEach(sel => {
            $(sel).style.opacity = locked ? '0.35' : '1';
            $(sel).style.cursor = locked ? 'default' : 'pointer';
        });
        $('#progress-bar').style.cursor = locked ? 'default' : 'pointer';
        if (S.isDJ) startSync();
        connMon();
        updateQueue();
        updateListenersL();
    }
    function goHome() {
        $('#home').classList.add('active');
        $('#room').classList.remove('active');
        $('#dj-controls').classList.add('hidden');
        setConn('');
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
        $('#btn-join').disabled = false;
        $('#btn-create').disabled = false;
        setJoinStatus('');
        S.joinBusy = false;
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
                    <div class="qi-size">${fmtSize(s.size)}${s.dur ? ' · ' + fmtTime(s.dur) : ''}</div>
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
    function updateListenersL() {
        const list = $('#listener-list');
        const n = (S.isDJ ? S.guests.size + 1 : S.others.size + 1);
        $('#listener-count').textContent = '👥 ' + n;
        let html = '';
        if (S.isDJ) {
            html += `<li class="listener-item"><div class="listener-avatar" style="background:var(--accent)">DJ</div><span class="listener-name">Kamu (DJ)</span><span class="listener-tag">DJ</span></li>`;
        } else {
            html += `<li class="listener-item"><div class="listener-avatar" style="background:var(--accent)">DJ</div><span class="listener-name">DJ</span><span class="listener-tag">DJ</span></li>`;
        }
        const roster = S.isDJ ? S.guests : S.others;
        roster.forEach((g) => {
            html += `<li class="listener-item"><div class="listener-avatar" style="background:${rndColor()}">${g.name[0]}</div><span class="listener-name">${esc(g.name)}</span></li>`;
        });
        list.innerHTML = html;
    }

    // ─── INIT ──────────────────────────────────────────────
    document.addEventListener('DOMContentLoaded', () => {
        S.audio = $('#audio');
        S.audio.volume = 0.75;

        S.audio.addEventListener('timeupdate', () => {
            if (S.isDJ) updateProgress(S.audio.currentTime, S.audio.duration);
        });
        S.audio.addEventListener('loadedmetadata', () => {
            if (S.isDJ) { S.duration = S.audio.duration; updateProgress(0, S.audio.duration); }
        });
        S.audio.addEventListener('ended', () => { if (S.isDJ) nextSong(); });

        $('#btn-create').addEventListener('click', createRoom);
        $('#btn-join').addEventListener('click', () => joinRoom($('#input-code').value.trim().toUpperCase()));
        $('#input-code').addEventListener('keydown', (e) => { if (e.key === 'Enter') $('#btn-join').click(); });

        $('#btn-leave').addEventListener('click', leaveRoom);
        $('#room-code').addEventListener('click', () => {
            navigator.clipboard.writeText(S.roomCode).then(() => toast('Kode disalin!')).catch(() => {});
        });

        $('#file-input').addEventListener('change', (e) => { handleFiles(e.target.files); e.target.value = ''; });
        const ua = $('#upload-area');
        ua.addEventListener('dragover',  (e) => { e.preventDefault(); ua.classList.add('dragover'); });
        ua.addEventListener('dragleave', ()  => ua.classList.remove('dragover'));
        ua.addEventListener('drop',      (e) => { e.preventDefault(); ua.classList.remove('dragover'); handleFiles(e.dataTransfer.files); });

        $('#btn-play').addEventListener('click', togglePlay);
        $('#btn-next').addEventListener('click', nextSong);
        $('#btn-prev').addEventListener('click', prevSong);

        let seeking = false;
        const pbar = $('#progress-bar');
        function seekFromEvent(e) {
            const r = pbar.getBoundingClientRect();
            seekTo(Math.max(0, Math.min(1, (e.clientX - r.left) / r.width)));
        }
        pbar.addEventListener('mousedown', (e) => { seeking = true; seekFromEvent(e); });
        document.addEventListener('mousemove', (e) => { if (seeking) seekFromEvent(e); });
        document.addEventListener('mouseup',  ()  => { seeking = false; });
        pbar.addEventListener('touchstart', (e) => { seeking = true; seekFromEvent(e.touches[0]); }, { passive: true });
        document.addEventListener('touchmove', (e) => { if (seeking) seekFromEvent(e.touches[0]); }, { passive: true });
        document.addEventListener('touchend',  ()  => { seeking = false; });

        $('#volume-slider').addEventListener('input', (e) => { S.audio.volume = e.target.value / 100; });

        const tapBtn = $('#tap-to-hear');
        tapBtn.addEventListener('click', () => {
            tryAutoPlay();
            setTimeout(() => { if (!S.audio.paused) hideTapToHear(); }, 600);
        });
        // Gestur pertama di mana saja membebaskan autoplay (browser asli mblokir tanpa interaksi)
        const unblockOnce = () => {
            try { if (S.audio && S.audio.srcObject) S.audio.play().catch(() => {}); } catch (_) {}
            if (S.audio && !S.audio.paused) hideTapToHear();
            document.removeEventListener('pointerdown', unblockOnce);
        };
        document.addEventListener('pointerdown', unblockOnce);

        $('#btn-shuffle').addEventListener('click', () => toast('Shuffle: segera'));
        $('#btn-repeat').addEventListener('click',  () => toast('Repeat: segera'));
        $('#btn-share-screen').addEventListener('click', toggleScreenShare);
        $('#btn-stop-screen').addEventListener('click', stopScreenShare);

        // Debug hook (untuk pengujian headless)
        window.__mtm = {
            get state() {
                return {
                    isDJ: S.isDJ, roomCode: S.roomCode, playing: S.playing,
                    songIdx: S.songIdx, duration: S.duration, gotStream: S.gotStream,
                    audioTime: S.audio.currentTime, audioPaused: S.audio.paused,
                    audioEnded: S.audio.ended, srcObject: !!S.audio.srcObject,
                    srcKind: S.audio.srcObject ? S.audio.srcObject.constructor.name : null,
                    tracks: S.audio.srcObject ? S.audio.srcObject.getTracks().map(t => t.kind) : [],
                    tapToHearHidden: !!$('#tap-to-hear').hidden,
                    pcState: S.isDJ ? Array.from(S.pcs).map(([id, pc]) => id + '=' + pc.connectionState).join(',')
                                    : (S.pc ? S.pc.connectionState : 'none'),
                    djRms: S.djRms,
                    queueLen: S.queue.length,
                    screenActive: !!S.screenActive,
                    screenStream: !!S.screenStream,
                    gotVideo: S.gotVideo,
                };
            },
            toast: (m) => toast(m),
        };
    });
})();