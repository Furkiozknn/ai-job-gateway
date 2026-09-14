# Katki Rehberi -- ai-job-gateway

Katkida bulunmak istedigin icin tesekkurler. Bu rehber, daha once hic
katki yapmamis birinin de ilk pull request'ini sorunsuz acabilmesi icin
yazildi.

Takildigin yerde issue acmaktan cekinme. **"Bu rehber su noktada
anlasilmiyor" da gecerli bir issue'dur** ve dokumantasyon hatasi olarak
islenir.

## 1. Gelistirme ortami

Ekosistem: **python**

```
git clone https://github.com/Furkiozknn/ai-job-gateway.git
cd ai-job-gateway
python -m venv .venv
.venv\Scripts\activate      # Linux/macOS: source .venv/bin/activate
pip install -e ".[dev]"   # yoksa: pip install -r requirements.txt
```

## 2. Degisikligi yapmadan once

- **Once issue ac.** Kucuk bir yazim hatasi disinda, dogrudan PR acmak
  yerine once ne yapmak istedigini yaz. Bu, ayni isi iki kisinin
  yapmasini ve kabul edilmeyecek bir isin bosa gitmesini onler.
- Var olan issue'lara bak; belki konu zaten tartisilmis.

## 3. Dal ve commit

Dal adi, ne yaptigini soylesin:

```
git checkout -b fix/bos-girdi-cokmesi
git checkout -b feat/json-cikti-secenegi
git checkout -b docs/kurulum-adimlari
```

Commit mesaji, **neden** yaptigini anlatsin -- ne yaptigin zaten diff'te
gorunuyor:

```
Bos girdide cokme yerine acik hata dondur

Kullanici bos dosya verdiginde IndexError firlatiyordu; hatanin
kaynagini gostermiyordu. Artik girdi dogrulanip anlamli bir mesajla
donuluyor.
```

## 4. Testler

```
pytest -q
```

Yeni davranis ekliyorsan **testini de ekle**. Hata duzeltiyorsan, once
hatayi yakalayan testi yaz, sonra duzelt -- boylece testin gercekten o
hatayi yakaladigindan emin olursun.

PR acildiginda su is akislari calisir: ci.yml. Hepsi yesil olmadan birlestirilmez.

## 5. Pull request

PR sablonu doldurulmayi bekleyen alanlar iceriyor. Ozellikle
**dogrulama** bolumu onemli: "testler gecti" yeterli degil, hangi komutu
calistirdigin ve ne gordugun yazilmali.

Inceleme sirasinda degisiklik istenmesi normaldir ve isin kotu oldugu
anlamina gelmez. Sorulari cevaplamak da katkinin bir parcasi.

## 6. Neyi kabul etmiyoruz

- Sadece bicimlendirme degistiren, davranisa dokunmayan buyuk diff'ler
  (kod tabaninin gecmisini okunmaz hale getiriyor).
- Gerekcesi yazilmamis yeni bagimliliklar.
- Testi olmayan yeni ozellikler.

## Davranis kurallari

Bu depoda [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md) gecerlidir.