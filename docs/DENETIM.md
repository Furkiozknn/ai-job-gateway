# Denetim: ai-job-gateway (30 Eylül 2026)

Yenilemeden önce `main` (`bcdcf33`, 0.1.0) üzerinde, bu makinede (Windows 11, Python 3.14 ve 3.12, uv, Git Bash) ölçüldü. Ölçülmeyen bir şey yazılmadı. Ham çıktılar depo dışında: `kanit/ai-job-gateway/{once,sonra}/komutlar.txt`.

## Kurulum ve ilk sonuç (boş klasör)

| Yol | Süre |
|---|---|
| `uvx --no-cache --from git+https://github.com/Furkiozknn/ai-job-gateway ai-job-gateway --help` (boş önbellek) | 19,2 s |
| aynısı, önbellek sıcak | 7,0 s |
| `git clone` + `uv sync` | 1,5 s + 4,4 s |
| `uv run ai-job-gateway ...` (her çağrı) | 4-8 s (Windows'ta uv her seferinde ortamı denetliyor) |

"Tek komut, bir dakikada ilk sonuç" tutuyor. README yalnızca klon + `uv sync` yolunu anlatıyordu; `uvx` yolu yoktu, eklendi.

## README komutları

| Komut | Sonuç |
|---|---|
| `serve`, `submit echo`, `submit mock-generate`, `/v1/capabilities`, `/health` | çalıştı |
| Idempotency-Key ile iki POST | aynı id (README'nin dediği gibi) |
| `webhook_url` = 169.254.169.254 | 422, README'nin dediği gibi |
| `submit` (istemci örneği, `JobGatewayClient`) | çalıştı |
| `uv run pytest` | 199 geçti, 6 atlandı (media eklentisi yok); yenileme sonrası 203 geçti, 6 atlandı |

## Bulunan sorunlar

| Girdi | Önce | Sorun |
|---|---|---|
| Sunucu yokken `submit echo ...` (README'nin "ikinci terminal" adımı, sunucu unutulursa) | ~50 satır `httpx.ConnectError` izi, çıkış 1 | **Hata.** İlk yanlışın en olası hali; ne yapılacağı söylenmiyor |
| `submit nope '{"a":1}'` | iki zincirli iz + `submission rejected (404): {"detail":...}` | ham JSON, iz, ipucu yok |
| `--timeout` aşımı | `TimeoutError` izi | iş sunucuda sürüyor, söylenmiyor |
| `--help` | alt komut listesi | açıklama, örnek, ortam değişkenleri (`AJG_API_KEY`, `AJG_WEBHOOK_SECRET`) yok; `--url`, `--timeout`, `--host`, `--port`, `capability` yardım metni yok |
| `serve` dolu portta | uvicorn hatası (Türkçe Windows'ta bozuk karakter), çıkış 3 | uvicorn'un davranışı; değiştirilmedi |

Gözlem (değiştirilmedi, sözleşme): `submit echo '{}'` boş gövde olduğu için 422 alır ("request body must be a non-empty JSON object").

## README bulguları

- İlk ekran uzun anlatı ve üreticisi olmayan 15 sn'lik GIF/MP4 ile açılıyordu; `docs/reel/*` ve elle yazılmış `assets/transcript.svg` (üreticisi yok) çıkarıldı, yerine gerçek oturum çıktısı kondu.
- "Ne zaman kullanılır / kullanılmaz" yoktu; eklendi.
- Testler: `project-meta.json` 204 diyor (media eklentisiyle ölçülmüş). Bu koşuda eklentiyi kurmak `MemoryError` verdi (8 GB RAM), 204 doğrulanamadı; meta dokunulmadı.
- Kütüphane API'si: tüm genel sınıflarda docstring var; paket `help()` çıktısında kullanım örneği yoktu, eklendi. Docstring'siz kalan: `JobGatewayClient.aclose`, `MockProvider.set_should_fail`, `SQLiteJobStore.close`.
- Günlük "Ekosistem denetimi" konusu (#19): bu depoya ait açık bulgu yok (yalnız profil deposunun kendi `ci.workflows` ayrışması).
