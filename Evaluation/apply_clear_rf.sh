#!/usr/bin/env bash
set -euo pipefail

# 1) Fichier RF CLEAR (path,rf)
CSV=/home/hduser/rf_10k/day2_clear_path_rf_10k.csv

# 2) Répertoires de données
DATA_DIR=/exp/day2/data
EC_DIR=/exp/day2/data_ec_clear
EC_POLICY="RS-6-3-1024k"

# 3) Poids pour WeightedWordCount
WEIGHTS_IN=/exp/day2/weights/weights.csv
WEIGHTS_OUT=/exp/day2/weights/weights_clear.csv

MAX_JOBS=10   # nombre max de setrep en parallèle

echo "Set $EC_POLICY erasure coding policy on $EC_DIR"
hdfs dfs -mkdir -p "$EC_DIR"
hdfs ec -setPolicy -path "$EC_DIR" -policy "$EC_POLICY" || true

########################################
# 1. Appliquer CLEAR sur HDFS
########################################

tail -n +2 "$CSV" | while IFS=',' read -r path rf; do
  path="${path//[$'\t\r\n ']}"
  rf="${rf//[$'\t\r\n ']}"

  [ -z "$path" ] && continue

  if [ "$rf" = "0" ]; then
    # Cas EC: fichier doit exister uniquement en EC
    rel="${path#$DATA_DIR/}"
    dest="$EC_DIR/$rel"
    echo "EC (RF=0) pour $path -> $dest"

    if hdfs dfs -test -e "$dest"; then
      echo "  -> déjà présent en EC, on supprime éventuellement la copie non EC"
      if hdfs dfs -test -e "$path"; then
        echo "  -> suppression de $path"
        hdfs dfs -rm -skipTrash "$path"
      fi
    else
      if hdfs dfs -test -e "$path"; then
        hdfs dfs -mkdir -p "$(dirname "$dest")"
        hdfs dfs -mv "$path" "$dest"
      else
        echo "  -> ni source ni dest, on skip (déjà traité ?)"
      fi
    fi

  else
    echo "Set RF=$rf pour $path"

    # setrep en parallèle, sans -w
    hdfs dfs -setrep "$rf" "$path" &

    # limiter le nombre de jobs simultanés
    while [ "$(jobs -r | wc -l)" -ge "$MAX_JOBS" ]; do
      wait -n
    done
  fi
done

# attendre la fin de tous les setrep en arrière plan
wait
echo "Tous les RF CLEAR ont été appliqués (commandes soumises)."

########################################
# 2. Générer weights_clear.csv pour CLEAR
########################################

echo "Génération de $WEIGHTS_OUT à partir de $WEIGHTS_IN et $CSV"

TMP_IN=/tmp/weights_in_$$.csv
TMP_OUT=/tmp/weights_clear_$$.csv

# Récupérer le fichier de poids depuis HDFS vers /tmp
hdfs dfs -cat "$WEIGHTS_IN" > "$TMP_IN"

python3 - "$CSV" "$TMP_IN" "$TMP_OUT" << 'EOF'
import sys, csv

rf_csv, win, wout = sys.argv[1:]

# Charger RF par path
rf_map = {}
with open(rf_csv, newline='') as f:
    r = csv.reader(f)
    next(r, None)  # sauter header éventuel
    for row in r:
        if not row:
            continue
        path = row[0].strip()
        rf = row[1].strip() if len(row) > 1 else ""
        rf_map[path] = rf

IN_PREFIX = "/exp/day2/data/"
EC_PREFIX = "/exp/day2/data_ec_clear/"

with open(win, newline='') as fin, open(wout, "w", newline='') as fout:
    r = csv.reader(fin)
    w = csv.writer(fout)

    header = next(r, None)
    if header:
        w.writerow(header)

    for row in r:
        if not row:
            continue
        path = row[0].strip()
        # Si ce path a RF=0 dans CLEAR, on le redirige vers data_ec_clear
        rf = rf_map.get(path)
        if rf == "0" and path.startswith(IN_PREFIX):
            new_path = EC_PREFIX + path[len(IN_PREFIX):]
            row[0] = new_path
        w.writerow(row)
EOF

# Remettre le fichier généré sur HDFS
hdfs dfs -mkdir -p "$(dirname "$WEIGHTS_OUT")"
hdfs dfs -put -f "$TMP_OUT" "$WEIGHTS_OUT"

rm -f "$TMP_IN" "$TMP_OUT"

echo "OK: $WEIGHTS_OUT généré sur HDFS."
