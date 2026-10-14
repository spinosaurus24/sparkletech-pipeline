"""
Pipeline SparkleTech - Collecte automatisée de données
Sources : Open-Meteo + Eurostat (nama_10_gdp + hbs_str_t211)
Destination : BigQuery (sparkletech-api-projet.Sparkle_Tech)
"""

import logging
import sys
from datetime import datetime

import pandas as pd
import requests
from google.cloud import bigquery
from pyjstat import pyjstat

# CONFIGURATION
PROJECT_ID = "sparkletech-api-projet"
DATASET_ID = "Sparkle_Tech"
LOG_FILE = "pipeline.log"

# LOGGING
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# MAPPINGS
VILLES = {
    "Lyon": (45.74906, 4.84789),
    "Paris": (48.8566, 2.3522),
    "Madrid": (40.4168, -3.7038),
    "Rome": (41.9028, 12.4964),
    "Berlin": (52.5200, 13.4050),
    "Bruxelles": (50.8503, 4.3517),
    "Luxembourg": (49.6116, 6.1319),
    "Amsterdam": (52.3676, 4.9041),
    "Dublin": (53.3498, -6.2603),
    "Lisbonne": (38.7223, -9.1393),
    "Vienne": (48.2082, 16.3738),
    "La Valette": (35.8997, 14.5148),
    "Copenhague": (55.6759, 12.5655),
    "Stockholm": (59.3294, 18.0687),
    "Helsinki": (60.1695, 24.9354),
    "Vilnius": (54.6872, 25.2797),
    "Riga": (56.9496, 24.1052),
    "Tallinn": (59.4370, 24.7536),
    "Varsovie": (52.2297, 21.0122),
    "Prague": (50.0755, 14.4378),
    "Budapest": (47.4979, 19.0402),
    "Bratislava": (48.1486, 17.1077),
    "Bucarest": (44.4268, 26.1025),
    "Sofia": (42.6977, 23.3219),
    "Zagreb": (45.8150, 15.9819),
    "Ljubljana": (46.0569, 14.5058),
    "Athènes": (37.9838, 23.7275),
    "Nicosie": (35.1666, 33.3666),
}

MAPPING_PAYS = {
    "Paris": "FR", "Lyon": "FR", "Madrid": "ES", "Rome": "IT",
    "Berlin": "DE", "Bruxelles": "BE", "Luxembourg": "LU",
    "Amsterdam": "NL", "Dublin": "IE", "Lisbonne": "PT",
    "Vienne": "AT", "La Valette": "MT", "Copenhague": "DK",
    "Stockholm": "SE", "Helsinki": "FI", "Vilnius": "LT",
    "Riga": "LV", "Tallinn": "EE", "Varsovie": "PL",
    "Prague": "CZ", "Budapest": "HU", "Bratislava": "SK",
    "Bucarest": "RO", "Sofia": "BG", "Zagreb": "HR",
    "Ljubljana": "SI", "Athènes": "EL", "Nicosie": "CY",
}

NOM_TO_CODE = {
    "Belgium": "BE", "Bulgaria": "BG", "Czechia": "CZ", "Denmark": "DK",
    "Germany": "DE", "Estonia": "EE", "Ireland": "IE", "Greece": "EL",
    "Spain": "ES", "France": "FR", "Croatia": "HR", "Italy": "IT",
    "Cyprus": "CY", "Latvia": "LV", "Lithuania": "LT", "Luxembourg": "LU",
    "Hungary": "HU", "Malta": "MT", "Netherlands": "NL", "Austria": "AT",
    "Poland": "PL", "Portugal": "PT", "Romania": "RO", "Slovenia": "SI",
    "Slovakia": "SK", "Finland": "FI", "Sweden": "SE",
}

CODES_PAYS = sorted(set(MAPPING_PAYS.values()))


# COLLECTE MÉTÉO
def collect_meteo():
    """Récupère la météo des 28 villes via Open-Meteo."""
    logger.info("Début collecte Open-Meteo")

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": ",".join(str(c[0]) for c in VILLES.values()),
        "longitude": ",".join(str(c[1]) for c in VILLES.values()),
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max",
        "timezone": "auto",
        "forecast_days": 7,
    }

    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        donnees = r.json()
        if isinstance(donnees, dict):
            donnees = [donnees]

        lignes = []
        for ville, data in zip(VILLES.keys(), donnees):
            daily = data["daily"]
            for i, date in enumerate(daily["time"]):
                lignes.append({
                    "ville": ville,
                    "date": date,
                    "temperature_min": daily["temperature_2m_min"][i],
                    "temperature_max": daily["temperature_2m_max"][i],
                    "precipitations_mm": daily["precipitation_sum"][i],
                    "vent_max_kmh": daily["wind_speed_10m_max"][i],
                })

        df = pd.DataFrame(lignes)
        df["date"] = pd.to_datetime(df["date"])
        df["geo"] = df["ville"].map(MAPPING_PAYS)

        # Ne garder que les dates où les 28 villes sont présentes
        compte_par_date = df.groupby("date")["ville"].nunique()
        dates_completes = compte_par_date[compte_par_date == len(VILLES)].index
        df = df[df["date"].isin(dates_completes)]

        logger.info(f"Météo filtrée : {len(df)} lignes, {df['ville'].nunique()} villes, {df['date'].nunique()} dates")
        return df

    except Exception as e:
        logger.error(f"Erreur collecte météo : {e}")
        raise


# COLLECTE EUROSTAT
def collect_eurostat():
    """Récupère PIB + dépenses ménages (nama_10_gdp)."""
    logger.info("Début collecte Eurostat nama_10_gdp")

    url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nama_10_gdp"
    params = {
        "format": "JSON",
        "lang": "EN",
        "geo": CODES_PAYS,
        "time": ["2019", "2020", "2021", "2022", "2023"],
        "unit": "CP_MEUR",
        "na_item": ["B1GQ", "P31_S14"],
    }

    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        df = pyjstat.Dataset.read(r.text).write("dataframe")

        df = df.rename(columns={
            "Time frequency": "time_freq",
            "Unit of measure": "unit_measure",
            "National accounts indicator (ESA 2010)": "na_indicator",
            "Geopolitical entity (reporting)": "geo",
            "Time": "time",
            "value": "value_meur",
        })
        df["time"] = df["time"].astype(int)

        logger.info(f"Eurostat collecté : {len(df)} lignes")
        return df

    except Exception as e:
        logger.error(f"Erreur collecte Eurostat : {e}")
        raise


# COLLECTE HBS
def collect_hbs():
    """Récupère la part des soins corporels (hbs_str_t211)."""
    logger.info("Début collecte Eurostat hbs_str_t211")

    url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/hbs_str_t211"
    params = {
        "format": "JSON",
        "lang": "EN",
        "geo": CODES_PAYS,
        "time": "2020",
        "coicop": "CP121",
    }

    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        df = pyjstat.Dataset.read(r.text).write("dataframe")

        df = df.rename(columns={
            "Time frequency": "time_freq",
            "Classification of individual consumption by purpose (COICOP)": "coicop",
            "Unit of measure": "unit_measure",
            "Geopolitical entity (reporting)": "geo_name",
            "Time": "time",
            "value": "val_soins_pour_mille",
        })
        df["geo"] = df["geo_name"].map(NOM_TO_CODE)

        logger.info(f"HBS collecté : {len(df)} lignes")
        return df

    except Exception as e:
        logger.error(f"Erreur collecte HBS : {e}")
        raise


# CHARGEMENT BIGQUERY
def charger_avec_deduplication(df, table_name, cle_metier):
    client = bigquery.Client(project=PROJECT_ID)
    table_cible = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    # 1. Lire les données existantes
    try:
        df_existant = client.query(f"SELECT * FROM `{table_cible}`").to_dataframe()
        logger.info(f"{table_name} : {len(df_existant)} lignes existantes lues")
    except Exception:
        df_existant = pd.DataFrame()
        logger.info(f"{table_name} : table n'existe pas encore")

    # 2. Normaliser la colonne date dans les DEUX DataFrames séparément
    def normaliser_date(d):
        if "date" in d.columns and len(d) > 0:
            d = d.copy()
            d["date"] = pd.to_datetime(d["date"], utc=True).dt.tz_localize(None).dt.normalize()
        return d

    df = normaliser_date(df)
    df_existant = normaliser_date(df_existant)

    # 3. Fusionner
    df_total = pd.concat([df_existant, df], ignore_index=True)

    # 4. Dédupliquer
    df_total = df_total.drop_duplicates(subset=cle_metier, keep="last")

    # 5. Réécrire
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df_total, table_cible, job_config=job_config)
    job.result()

    logger.info(f"{table_name} : {len(df_total)} lignes écrites (dédupliquées)")


# MAIN
def main():
    logger.info("=" * 60)
    logger.info("DÉMARRAGE DU PIPELINE SPARKLETECH")
    logger.info("=" * 60)

    try:
        df_meteo = collect_meteo()
        df_eurostat = collect_eurostat()
        df_hbs = collect_hbs()

        charger_avec_deduplication(df_meteo, "meteo", ["ville", "date"])
        charger_avec_deduplication(df_eurostat, "eurostat", ["geo", "time", "na_indicator"])
        charger_avec_deduplication(df_hbs, "hbs_soins_corporels", ["geo", "coicop"])
        logger.info("PIPELINE TERMINÉ AVEC SUCCÈS")

    except Exception as e:
        logger.error(f"PIPELINE ÉCHOUÉ : {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()