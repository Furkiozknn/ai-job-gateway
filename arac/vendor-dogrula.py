#!/usr/bin/env python3
"""`gateway_poll.py` hâlâ kaynağındaki dosyayla aynı mı?

Bu modül dört depoda birebir aynı: ai-job-gateway (sahibi), ai-workflow-engine,
model-comparison-harness, prompt-template-manager. Tek fark, paket adı yüzünden
oluşan docstring satırları.

Kopyalamak bilinçli bir karar — dört projeden hiçbiri diğerine bağımlı olmak
zorunda kalmıyor. Ama kopyalamanın bilinen bedeli var: kopyalar sessizce
ayrışır. Birinde düzeltilen bir sınır durumu diğer üçünde durmaya devam eder,
ve kimse fark etmez çünkü her depo kendi kopyasına karşı yeşil kalır.

Bu betik o sessizliği kapatıyor. Kanonik dosyayı ai-job-gateway'in ana dalından
çekiyor, paket adı farkını normalleştiriyor ve karşılaştırıyor.

    python3 arac/vendor-dogrula.py             # ağdan kanonik kopyayi cek
    python3 arac/vendor-dogrula.py --dosya X   # elindeki bir kopyayla karsilastir
    python3 arac/vendor-dogrula.py --parmak    # yerel kopyanin parmak izini yaz

Ağ yoksa betik **atlıyor, geçmiyor**: "bakamadım" ile "aynı" aynı şey değil ve
bir sürüm kapısının ikisini karıştırması onu kapı olmaktan çıkarır. Çıkış kodu
0 (aynı), 1 (ayrışmış), 2 (bakılamadı).
"""

from __future__ import annotations

import argparse
import difflib
import hashlib
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

KANONIK = (
    "https://raw.githubusercontent.com/Furkiozknn/ai-job-gateway/main/"
    "src/ai_job_gateway/gateway_poll.py"
)

#: Paket adlari disinda hicbir fark kabul edilmiyor.
PAKETLER = (
    "ai_job_gateway", "ai-job-gateway",
    "ai_workflow_engine", "ai-workflow-engine",
    "model_comparison_harness", "model-comparison-harness",
    "prompt_template_manager", "prompt-template-manager",
)


def yerel_kopya(kok: Path) -> Path:
    adaylar = sorted(kok.glob("src/*/gateway_poll.py"))
    if not adaylar:
        raise SystemExit("bu depoda src/*/gateway_poll.py yok")
    if len(adaylar) > 1:
        raise SystemExit("birden fazla kopya var: %s" % ", ".join(str(a) for a in adaylar))
    return adaylar[0]


def normalize(metin: str) -> str:
    """Paket adlarini tek bir yer tutucuya indirger, satir sonlarini duzler."""
    for ad in PAKETLER:
        metin = metin.replace(ad, "PAKET")
    return metin.replace("\r\n", "\n").strip() + "\n"


def parmak(metin: str) -> str:
    return hashlib.sha256(normalize(metin).encode("utf-8")).hexdigest()[:16]


def kanonik_getir(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "vendor-dogrula"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return r.read().decode("utf-8")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dosya", help="kanonik kopyayi agdan cekmek yerine bu dosyadan oku")
    ap.add_argument("--parmak", action="store_true", help="yalnizca yerel parmak izini yaz")
    ap.add_argument("--url", default=KANONIK)
    args = ap.parse_args(argv)

    kok = Path(__file__).resolve().parent.parent
    yerel_yol = yerel_kopya(kok)
    yerel = yerel_yol.read_text(encoding="utf-8")

    if args.parmak:
        print("%s  %s" % (parmak(yerel), yerel_yol.relative_to(kok)))
        return 0

    if args.dosya:
        kanonik = Path(args.dosya).read_text(encoding="utf-8")
        kaynak = args.dosya
    else:
        try:
            kanonik = kanonik_getir(args.url)
            kaynak = args.url
        except (urllib.error.URLError, OSError, TimeoutError) as ex:
            print("ATLANDI: kanonik kopyaya bakilamadi (%s)" % ex)
            print("  'bakamadim' ile 'ayni' ayni sey degil; bu kosu bir sey kanitlamadi.")
            return 2

    a, b = normalize(kanonik), normalize(yerel)
    if a == b:
        print("ayni  %s  (parmak %s)" % (yerel_yol.relative_to(kok), parmak(yerel)))
        print("      kaynak: %s" % kaynak)
        return 0

    print("AYRISMIS  %s" % yerel_yol.relative_to(kok))
    print("  kanonik parmak: %s" % parmak(kanonik))
    print("  yerel parmak  : %s" % parmak(yerel))
    print("  kaynak: %s" % kaynak)
    print()
    fark = list(difflib.unified_diff(
        a.splitlines(), b.splitlines(),
        fromfile="kanonik/gateway_poll.py", tofile="yerel/gateway_poll.py",
        lineterm="", n=2,
    ))
    print("\n".join(fark[:60]))
    if len(fark) > 60:
        print("... (%d satir daha)" % (len(fark) - 60))
    print()
    print("  Bu dosya dort depoda birebir ayni tutuluyor. Bir tarafta duzeltilen")
    print("  sinir durumu digerlerinde durmaya devam ederse kimse fark etmez.")
    print("  Degisiklik dogruysa ai-job-gateway'e yapilip buraya kopyalanmali;")
    print("  degilse geri alinmali.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
