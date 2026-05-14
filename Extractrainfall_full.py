from ftplib import FTP
import os
import csv
import re
import traceback
from datetime import datetime
import pandas as pd

# =========================================================
# SETTINGS
# =========================================================
FTP_HOST = "10.10.233.209"
FTP_USER = "ATS-RMA"
FTP_PASS = "N2s1-1TS!"   # put your real password here
FTP_SOURCE_FOLDER = "/files"

BASE_FOLDER = r"C:\Users\Gahigi\Documents\Meteo_project"

RAW_FOLDER = os.path.join(BASE_FOLDER, "13_05_2025")
OUTPUT_FOLDER = os.path.join(BASE_FOLDER, "14_05_2025")
LOGS_FOLDER = os.path.join(OUTPUT_FOLDER, "logs")

FTP_LOG_FILE = os.path.join(BASE_FOLDER, "ftp_transfer_log.txt")
DOWNLOADED_TRACK_FILE = os.path.join(BASE_FOLDER, "downloaded_files.txt")
PROCESSED_LOG_FILE = os.path.join(LOGS_FOLDER, "processed_files.log")
EXCEL_REPORT = os.path.join(OUTPUT_FOLDER, "rainfall_total_report.xlsx")

START_DATETIME = datetime(2026, 5, 12, 8, 0)
END_DATETIME = datetime(2026, 5, 13, 8, 0)

os.makedirs(RAW_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
os.makedirs(LOGS_FOLDER, exist_ok=True)

# =========================================================
# STATION MAP
# =========================================================
station_map = {
    "015358": "AKAGERA NYUNGWE.txt",
    "015355": "AKAGERA RWISIRABO.txt",
    "015369": "Bakokwe.txt",
    "010060": "Bugarura.txt",
    "015361": "Bungwe.txt",
    "010033": "Butare.txt",
    "015372": "Bweyeye.txt",
    "010182": "Bwisige.txt",
    "010034": "Byimana.txt",
    "010039": "Byumba.txt",
    "015363": "Cyanika Border Market.txt",
    "015362": "Cyeru.txt",
    "010183": "Cyumba.txt",
    "015357": "Gabiro.txt",
    "010002": "Gacurabwenge.txt",
    "018763": "GATENGA.txt",
    "015348": "Gihinga.txt",
    "015347": "GIKOMERO.txt",
    "010035": "Gikongoro.txt",
    "010037": "Gisenyi.txt",
    "015375": "Gishyita.txt",
    "018759": "GITEGA.txt",
    "018762": "JALI.txt",
    "010059": "Kabarore.txt",
    "015370": "Kaduha.txt",
    "010036": "Kamembe.txt",
    "015368": "Kavumu.txt",
    "010058": "Kawangire.txt",
    "010032": "Kazo.txt",
    "015359": "Kenjobe.txt",
    "010046": "Kinigi.txt",
    "015351": "Kirehe.txt",
    "018758": "KIVUGIZA.txt",
    "015353": "Mahama.txt",
    "015349": "Mamba.txt",
    "015360": "MATIMBA.txt",
    "015346": "Mayange.txt",
    "010052": "Muhungwe.txt",
    "015356": "Mukarange.txt",
    "010184": "Mulindi Tea.txt",
    "017293": "Nasho_Mpanga.txt",
    "015354": "Ndego.txt",
    "018761": "NDUBA.txt",
    "015367": "Ngororero.txt",
    "015364": "Ntaruka.txt",
    "015371": "Nyabimata.txt",
    "010053": "Nyabirasi.txt",
    "015373": "NYABITIMBO.txt",
    "010031": "Nyagatare.txt",
    "017292": "Nyirangarama.txt",
    "010055": "Nzaratsi.txt",
    "010047": "Rubona.txt",
    "018760": "RUBUNGO.txt",
    "015366": "Rusasa.txt",
    "010001": "Rushashi.txt",
    "018764": "RUSORORO.txt",
    "015352": "RUSUMO POWER PLANT.txt",
    "010004": "Rwamagana.txt",
    "015350": "Rweru.txt",
    "015365": "Tamira.txt",
    "015374": "Wisumo.txt",
    "010057": "Zaza.txt"
}

mapping = {
    (10, 7): "rainfall"
}

columns = ["datetime", "rainfall"]

# =========================================================
# LOGGING
# =========================================================
def write_main_log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"[{timestamp}] {message}"
    print(full_message)

    with open(FTP_LOG_FILE, "a", encoding="utf-8") as log:
        log.write(full_message + "\n")


def load_text_file_as_set(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f if line.strip())
    return set()


def append_to_text_file(path, value):
    with open(path, "a", encoding="utf-8") as f:
        f.write(value + "\n")


# =========================================================
# FTP DOWNLOAD PART
# =========================================================
def extract_datetime_from_filename(filename):
    try:
        name = os.path.splitext(filename)[0]

        if len(name) < 18:
            return None

        year = int(name[6:10])
        month = int(name[10:12])
        day = int(name[12:14])
        hour = int(name[14:16])
        minute = int(name[16:18])

        return datetime(year, month, day, hour, minute)

    except:
        return None


def download_files_from_ftp():
    ftp = None
    downloaded_files = load_text_file_as_set(DOWNLOADED_TRACK_FILE)
    new_files_count = 0

    try:
        write_main_log("CONNECTING TO FTP SERVER...")
        ftp = FTP(FTP_HOST, timeout=120)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.cwd(FTP_SOURCE_FOLDER)

        write_main_log("FTP LOGIN SUCCESSFUL")

        files = ftp.nlst()

        for file in files:
            if not file.lower().endswith(".txt"):
                continue

            if file in downloaded_files:
                continue

            file_datetime = extract_datetime_from_filename(file)

            if file_datetime is None:
                write_main_log(f"INVALID FILE FORMAT: {file}")
                continue

            if not (START_DATETIME <= file_datetime <= END_DATETIME):
                continue

            local_path = os.path.join(RAW_FOLDER, file)

            with open(local_path, "wb") as local_file:
                ftp.retrbinary(f"RETR {file}", local_file.write)

            if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                append_to_text_file(DOWNLOADED_TRACK_FILE, file)
                new_files_count += 1
                write_main_log(f"DOWNLOADED: {file}")
            else:
                write_main_log(f"EMPTY OR FAILED FILE: {file}")

        ftp.quit()
        write_main_log(f"FTP DOWNLOAD FINISHED. NEW FILES: {new_files_count}")

    except Exception as e:
        write_main_log("FTP ERROR OCCURRED")
        write_main_log(str(e))
        traceback.print_exc()

        try:
            if ftp:
                ftp.quit()
        except:
            pass


# =========================================================
# RAINFALL EXTRACTION PART
# =========================================================
def smart_split(line):
    return re.split(r"[;,]+", line.strip())


def clean_value(v):
    v = v.strip()
    if v in ["", "*", "NaN", "nan", "NULL", "null"]:
        return None
    return v


def get_station_file(filename):
    for prefix, station_file in station_map.items():
        if filename.startswith(prefix):
            return station_file
    return None


def write_invalid_value_log(station, timestamp, field, value):
    log_file = os.path.join(LOGS_FOLDER, "invalid_values.csv")
    file_exists = os.path.isfile(log_file)

    with open(log_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["Station", "datetime", "parameter", "value"]
        )

        if not file_exists:
            writer.writeheader()

        writer.writerow({
            "Station": station,
            "datetime": timestamp,
            "parameter": field,
            "value": value
        })


def is_valid(field, value):
    try:
        v = float(value)
    except:
        return False

    ranges = {
        "rainfall": (0, 40)
    }

    if field in ranges:
        min_v, max_v = ranges[field]
        return min_v <= v <= max_v

    return True


def process_file(file_path, processed_files):
    filename = os.path.basename(file_path)

    if filename in processed_files:
        print(f"⏩ Already processed: {filename}")
        return

    station_file = get_station_file(filename)

    if not station_file:
        print(f"⚠ No station mapping: {filename}")
        return

    output_path = os.path.join(OUTPUT_FOLDER, station_file)
    data_by_time = {}

    with open(file_path, "r", encoding="utf-8", errors="ignore") as f_in:
        for line in f_in:
            line = line.replace("#", "").strip()
            parts = smart_split(line)

            if len(parts) < 10 or parts[0] != "S":
                continue

            try:
                timestamp = (
                    f"{parts[7]}-{parts[6].zfill(2)}-{parts[5].zfill(2)} "
                    f"{int(parts[2]):02d}:{int(parts[3]):02d}:{int(parts[4]):02d}"
                )
            except:
                continue

            if timestamp not in data_by_time:
                data_by_time[timestamp] = {c: "" for c in columns}
                data_by_time[timestamp]["datetime"] = timestamp

            row = data_by_time[timestamp]
            data = parts[8:]
            i = 0

            while i + 2 < len(data):
                try:
                    m = int(data[i])
                    p = int(data[i + 1])
                    v = clean_value(data[i + 2])

                    if v is not None and (m, p) in mapping:
                        field = mapping[(m, p)]

                        if is_valid(field, v):
                            row[field] = v
                        else:
                            write_invalid_value_log(
                                station_file.replace(".txt", ""),
                                timestamp,
                                field,
                                v
                            )

                    i += 3

                except:
                    i += 1

    file_exists = os.path.isfile(output_path)

    with open(output_path, "a", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=columns)

        if not file_exists or os.path.getsize(output_path) == 0:
            writer.writeheader()

        for row in data_by_time.values():
            if all(v == "" for k, v in row.items() if k != "datetime"):
                continue
            writer.writerow(row)

    append_to_text_file(PROCESSED_LOG_FILE, filename)
    processed_files.add(filename)

    print(f"✔ Processed: {filename}")


def process_all_downloaded_files():
    processed_files = load_text_file_as_set(PROCESSED_LOG_FILE)

    print("🚀 Processing downloaded rainfall files...")

    for file in os.listdir(RAW_FOLDER):
        if file.lower().endswith(".txt"):
            process_file(os.path.join(RAW_FOLDER, file), processed_files)

    print("✅ Rainfall extraction finished.")


# =========================================================
# EXCEL REPORT PART
# =========================================================
def generate_excel_report():
    report_rows = []

    for file in os.listdir(OUTPUT_FOLDER):
        if not file.lower().endswith(".txt"):
            continue

        station_name = file.replace(".txt", "")
        file_path = os.path.join(OUTPUT_FOLDER, file)

        try:
            df = pd.read_csv(file_path)

            if "rainfall" not in df.columns:
                continue

            df["rainfall"] = pd.to_numeric(df["rainfall"], errors="coerce")

            total_rainfall = df["rainfall"].sum()
            record_count = df["rainfall"].notna().sum()

            report_rows.append({
                "Station": station_name,
                "Number_of_Records": record_count,
                "Total_Rainfall": total_rainfall
            })

        except Exception as e:
            print(f"Could not summarize {file}: {e}")

    report_df = pd.DataFrame(report_rows)

    if report_df.empty:
        print("⚠ No rainfall data found for Excel report.")
        return

    report_df = report_df.sort_values("Total_Rainfall", ascending=False)

    with pd.ExcelWriter(EXCEL_REPORT, engine="openpyxl") as writer:
        report_df.to_excel(writer, index=False, sheet_name="Total Rainfall")

    print(f"✅ Excel report generated: {EXCEL_REPORT}")


# =========================================================
# MAIN PROGRAM
# =========================================================
if __name__ == "__main__":

    write_main_log("======================================")
    write_main_log("FULL FTP + RAINFALL PROCESS STARTED")
    write_main_log("======================================")

    write_main_log(f"START DATE: {START_DATETIME}")
    write_main_log(f"END DATE: {END_DATETIME}")

    download_files_from_ftp()
    process_all_downloaded_files()
    generate_excel_report()

    write_main_log("✅ FULL PROCESS COMPLETED")