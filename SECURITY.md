# Guvenlik Politikasi

## Desteklenen surumler

Bu proje icin yalnizca **varsayilan dalin ($ana) son hali** desteklenir.
Eski etiketlere geriye donuk yama uygulanmaz; guvenlik duzeltmesi her
zaman ileriye dogru, yeni bir surumle yayinlanir.

## Acik bildirimi

**Guvenlik aciklarini normal issue olarak acmayin.** Acik issue, yama
hazir olmadan once sorunu herkese duyurur.

Bunun yerine GitHub'in ozel bildirim kanalini kullanin:

1. Bu deponun **Security** sekmesine gidin.
2. **Report a vulnerability** baglantisina tiklayin.
3. Formu doldurun. Bildirim yalnizca depo sahibine gorunur.

Bu kanal kapaliysa veya calismiyorsa, GitHub uzerinden depo sahibine
dogrudan mesaj gonderip ozel bir kanal talep edin.

## Bildirimde ne olmali

Faydali bir bildirim su dordunu icerir:

- **Etkilenen surum veya commit.** `git rev-parse HEAD` ciktisi ideal.
- **Yeniden uretme adimlari.** Mumkunse en kucuk calisan ornek.
- **Etki.** Saldirgan bu acikla ne yapabiliyor -- veri okuma, kod
  calistirma, servis disi birakma?
- **Ortam.** Isletim sistemi, calisma zamani surumu.

## Sureclerin takvimi

| Adim | Hedef sure |
|---|---|
| Bildirimin alindiginin teyidi | 72 saat |
| Ilk degerlendirme (gecerli mi, ne kadar ciddi) | 7 gun |
| Duzeltme veya azaltma plani | 30 gun |
| Kamuya aciklama | Duzeltme yayinlandiktan sonra |

Bu bir taahhut degil hedeftir; tek kisilik bir projede gecikme olabilir.
Sure asilirsa bildirimi yapan kisiye durum bilgisi verilir.

## Kapsam disi

Asagidakiler guvenlik acigi olarak islenmez:

- Otomatik tarayici ciktilarinin, gercek bir saldiri senaryosu
  gosterilmeden dogrudan yapistirilmasi.
- Yalnizca kullanicinin kendi makinesinde, kendi yetkisiyle
  yapabilecegi islemler.
- Bagimliliklardaki, bu projenin kullandigi kod yolunu etkilemeyen
  bildirilmis acikla. (Yine de bildirmek isterseniz normal issue uygun.)

## Tesekkur

Sorumlu bildirim yapan kisiler, aksini istemedikleri surece duzeltme
notlarinda anilir.