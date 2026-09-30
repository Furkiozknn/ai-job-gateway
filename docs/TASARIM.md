# Tasarım: ai-job-gateway arayüz yenilemesi

## Hedef
İlk ekranda: tek cümle tanım, tek komutlu kurulum (`uvx --from git+... ai-job-gateway serve`), gerçek çıktılı kısa oturum, "ne zaman kullanılır / kullanılmaz". Yanlış kullanımda iz yerine tek satır, ne yapılacağı belli hata.

## Önce / sonra
| | Önce | Sonra |
|---|---|---|
| README girişi | GIF + uzun anlatı + klon/sync | tek cümle, `uvx` komutu, gerçek oturum bloğu, kullanım sınırları |
| Sunucusuz `submit` | ~50 satır iz | `error: cannot reach a gateway at URL ... Start one ... ai-job-gateway serve`, çıkış 1 |
| Bilinmeyen capability | iz + ham JSON | `error: the gateway rejected the job (HTTP 404): unknown capability: 'nope'` + `/v1/capabilities` ipucu |
| `--help` | komut listesi | açıklama, hızlı başlangıç, ortam değişkenleri, her seçenek için metin |
| Test | 199 | 203 |

## Akış
`serve` (terminal 1) -> `submit CAPABILITY 'JSON'` (terminal 2): gönder, kimliği yaz, yokla, sonucu JSON bas. Başarıda 0, her hata türünde 1. Sunucu davranışı, HTTP sözleşmesi ve kütüphane API'si değişmedi.

## Görsel kimlik
Terminal videosu FRK-OS renkleri (`sosyal/uret/tema.mjs`: siyah #0e0d0b, krem #f1ece2, sarı #ffc21a) ve yerel JetBrains Mono ile; README'de görsel yok, düz metin bloğu (kopyalanabilir, kaynağı gerçek çıktı).
