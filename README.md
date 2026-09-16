# 🎵 Music Together

Streaming musik bareng **real-time** — kualitas jernih, **tanpa VPS**.

Pakai **WebRTC** (peer-to-peer): tidak ada server musik. Yang ada cuma **signaling gratis via broker MQTT publik** (EMQX / HiveMQ) untuk saling mengenalkan antar device, setelah itu audio langsung lewat P2P antar pengguna.

## Cara Kerja

```
  DJ (HP/laptop)              Pendengar (HP siapa saja)
  ─────────────               ──────────────────────────
  1. Buat ruangan             2. Masukkan kode → Gabung
  3. Upload file musik
     (MP3/WAV/FLAC/OGG)
  4. Audio ditangkap (WebRTC) ══▶ 4. Terima audio live
  5. Sync state 500ms ════════╤══▶ 5. Ikut sinkron (play/
     (posisi, play/pause)     │     pause otomatis)
                             ─┴─▶ Kualitas: 48kHz stereo
```

## Fitur

- **Kualitas tinggi (HD)** — Opus 48kHz **stereo penuh** 512kbps (SDP munging `maxaveragebitrate=512000;stereo=1`), ditambah `contentHint=music` pada track audio agar browser memprioritaskan kualitas audio
- **Sinkron real-time** — semua pendengar dengar lagu yang sama dalam selisih ±100ms (uji: drift < 250ms selama 60 detik tanpa koreksi)
- **Bagikan layar + audio** — DJ bisa **share screen** (mis. video YouTube/live streaming) lengkap dengan audio sistemnya (Chrome/Edge). Tanpa audio layar (Firefox/Safari), musik DJ jadi backup audio-nya. Pendengar melihat panel video + ikon LIVE.
- **Tanpa VPS** — signaling MQTT publik gratis + hosting statis gratis
- **Bisa dari HP** — buka link, langsung gabung, tanpa install
- **DJ package** — upload dari HP/laptop, drag & drop, antrian, next/prev, seek, hapus lagu
- **UI gelap modern** — mobile-first, cocok night mode

## Cara Pakai

**Cara paling cepat (uji lokal):**
1. Aktifkan server lokal di folder ini:
   ```
   python -m http.server 8000
   ```
2. Buka `http://localhost:8000` di **dua tab browser** (atau 2 device)
3. Tab 1 → klik **Buat Ruangan** → dapat kode 6 huruf
4. Tab 2 → masukkan kode → **Gabung**
5. Di Tab 1, upload lagu → play → Tab 2 ikut terdengar sinkron

**Cara jalan di internet (gratis, GitHub Pages):**

1. Buat repo baru di GitHub (misal `music-together`)
2. Upload seluruh isi folder ini (semua file)
3. Setelan repo → **Pages** → Branch `main` → folder `/ (root)` → Save
4. Tunggu 1-2 menit → dapat link `https://username.github.io/music-together`
5. Share link itu ke teman — siapa pun bisa buka & gabung

> Catatan: karena hostingnya halaman statis + broker MQTT publik gratis, **tidak ada biaya server sekalipun dipakai ramai**.

## Sinkronisasi — Bagaimana Bisa Presisi

| Mekanisme | Detail |
|-----------|--------|
| Media channel (WebRTC) | DJ main satu lagu, audio ditangkap & dikirim live ke semua pendengar. **Satu stream = otomatis sama**. |
| Latency compensation | DJ kirim `{time, playing, ts}` tiap 500ms. Pendengar hitung `target = time + (now - ts)`. |
| Drift guard `< 250ms` | Kalau selisih melewati ambang, posisi dikoreksi (khusus lagu offline). Untuk live stream, seek di-skip — stream sudah sinkron secara alami. |
| Play/pause sync | Status play/pause ikut DJ. Pendengar tidak bisa play kalau DJ pause. |
| Join mid-song | Pendengar baru langsung menerima stream + posisi saat ini. |

## Arsitektur

```
music-together/
├── index.html     → halaman (home + ruangan)
├── css/style.css  → tema gelap modern, responsive
└── js/app.js      → MQTT signaling, WebRTC P2P, sync engine, audio, UI
```

- **Signaling: broker MQTT publik** — wss ke `broker.emqx.io:8084` lalu fallback `broker.hivemq.com:8884`. Topik ruangan `mtm/{KODE}`. VPS 0.
- **WebRTC manual** — DJ: `addTrack` (audio capture dari `<audio>` via `MediaStreamDestination`). Pendengar: transceiver `recvonly`, terima aliran audio DJ langsung P2P.
- **Presence** — DJ beat tiap 4s, pendengar beat tiap 9s, saling deteksi online/offline + roster pendengar.
- **Kode ruangan** — 6 karakter (tanpa 0/O/1/I) → ruang unik.

## Ketahanan

- **Broker failover** — kalau broker pertama tidak bisa dihubungi, otomatis ganti ke broker kedua.
- **Auto reconnect** — koneksi signaling putus → nyambung sendiri; WebRTC yang sudah jalan tidak terputus.
- **Retry join** — jika audio belum diterima, pendengar mengirim ulang offer tiap 6 detik sampai stream masuk.
- **Multi-STUN** — 3 server STUN (Google ×2, Twilio, Cloudflare) memaksimalkan peluang koneksi P2P tembus NAT.
- **Self-echo filter** — pesan MQTT punya `src`; pesan sendiri diabaikan (broker meng-echo).

## Limitasi (v2)

- File audio: maks 50MB per lagu, dari device DJ.
- DJ harus online — kalau DJ keluar, ruangan bubar.
- Kualitas = kompresi WebRTC Opus (HD 512kbps stereo via SDP munging — setara Spotify).
- **NAT simetris** (sering di jaringan seluler) tidak bisa ditembus P2P murni — butuh TURN relay. Kalau gagal, pastikan DJ dan pendengar di **Wi-Fi yang sama**.

## Troubleshooting

**Klik tombol "🔊 Klik untuk mendengar"** — browser memblokir autoplay musik tanpa interaksi pertama dari user. Tap sekali pada tombol (atau ketuk mana saja di layar), audio langsung jalan.

**Pendengar tidak dapat audio?** — Buka console browser pendengar (F12), ketik `JSON.stringify(window.__mtm.state,null,2)`. Cek:
- `pcState: "connected"` → WebRTC OK → masalah autoplay (lihat di atas).
- `pcState: "failed"` → WebRTC gagal. Solusi: DJ dan pendengar harus di **jaringan yang sama** (satu WiFi/LAN) atau gunakan jaringan rumah yang bukan seluler.
- `srcObject: true, tracks: ["audio"]` → stream sudah sampai, kembali ke langkah autoplay.
- `gotStream: false` → stream belum sampai, tunggu atau cek jaringan.

## Roadmap

- [ ] Share URL notifikasi (copy link penuh)
- [ ] Persist queue (localStorage)
- [ ] Mode repeat / shuffle
- [ ] Chat bareng
- [ ] Guide TURN sederhana (relay untuk NAT simetris)
- [ ] Restream ke Telegram/Discord (opsional, pakai bridge)

---

Dibuat tanpa VPS — real-time, gratis, buka di browser siapa saja.