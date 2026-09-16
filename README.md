# 🎵 Music Together

Streaming musik bareng **real-time** — kualitas jernih, **tanpa VPS**.

Bisa-nya pake **WebRTC** (peer-to-peer): tidak ada server musik. Yang ada cuma signaling gratis dari PeerJS buat perkenalan antar device, setelah itu audio langsung lewat P2P antar pengguna.

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

- **Kualitas jernih** — Opus 48kHz stereo (bukan kualitas telepon. ±128kbps, setara Spotify high)
- **Sinkron real-time** — semua pendengar dengar lagu yang sama dalam selisih ±100ms (uji: drift < 250ms selama 60 detik tanpa koreksi)
- **Tanpa VPS** — PeerJS signaling gratis + hosting gratis
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

> Kalau pakai `file://` (double-click) juga bisa, tapi server lokal lebih baik.

**Cara jalan di internet (gratis, GitHub Pages):**

1. Buat repo baru di GitHub (misal `music-together`)
2. Upload seluruh isi folder ini (semua file)
3. Setelan repo → **Pages** → Branch `main` → folder `/ (root)` → Save
4. Tunggu 1-2 menit → dapat link `https://username.github.io/music-together`
5. Share link itu ke teman — siapa pun bisa buka & gabung

> Catatan: karena hostingnya halaman statis + PeerJS cloud, **tidak ada biaya server sekalipun dipakai ramai**.

## Sinkronisasi — Bagaimana Bisa Presisi

| Mekanisme | Detail |
|-----------|--------|
| Media channel (WebRTC) | DJ main satu lagu, audio ditangkap & dikirim live ke semua pendengar. **Satu stream = otomatis sama**. |
| Latency compensation | DJ kirim `{time, ts}` tiap 500ms. Pendengar hitung `target = time + (now - ts)`. |
| Drift guard `< 250ms` | Kalau selisih melewati ambang, posisi dikoreksi. |
| Play/pause sync | Status play/pause ikut DJ. Pendengar tidak bisa play kalau DJ pause. |
| Join mid-song | Pendengar baru langsung menerima stream + posisi saat ini. |

## Arsitektur

```
music-together/
├── index.html     → halaman (home + ruangan)
├── css/style.css  → tema gelap modern, responsive
└── js/app.js      → PeerJS P2P, sync engine, audio, UI
```

- **PeerJS CDN** — signaling server gratis (`0.peerjs.com`), VPS 0.
- **Media element → `createMediaElementSource()`** → `MediaStreamDestination` → dikirim via `peer.call()`.

## Ketahanan

- **Retry otomatis saat join** — jika discovery gagal (umum di server sinyal gratis), pendengar mengulang hingga 4x dengan jeda bertambah → tidak akan "terputus" di tengah pencarian.
- **Multi-STUN** — 3 server STUN (Google ×2, Twilio, Cloudflare) memaksimalkan peluang koneksi P2P tembus NAT.
- **Reconnect** — kalau koneksi putus, pendengar otomatis nyambung lagi ke ruangan yang sama.
- **Kode unik** — collision kode ruangan dideteksi & kode baru dibuat otomatis.

## Limitasi (v1)

- File audio: maks 50MB per lagu, dari device DJ.
- DJ harus online — kalau DJ keluar, ruangan bubar.
- Kualitas = kompresi WebRTC Opus (masih jauh lebih bagus dari voice chat; FLAC asli tidak didukung live-stream).
- Kalau teman di jaringan berbeda perlu internet (WebRTC butuh STUN public — sudah termasuk default).

## Roadmap

- [ ] Share URL notifikasi (copy link penuh)
- [ ] Persist queue (localStorage)
- [ ] Mode repeat / shuffle
- [ ] Chat bareng
- [ ] Restream ke Telegram/Discord (opsional, pakai bridge)

---

Dibuat tanpa VPS — real-time, gratis, buka di browser siapa saja.