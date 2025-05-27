import requests
from bs4 import BeautifulSoup
import os
import time
import re
from datetime import datetime
 
# --- Cấu hình ---
BASE_URL = "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FGJ&QO_fu146_anzr=b0-gvzr"
OUTPUT_DIR = "bts_downloads"
START_YEAR = 2015
END_YEAR = 2024 # Bao gồm cả năm này
 
# Các cột cần chọn (dựa trên name attribute của input checkbox trong HTML)
REQUIRED_COLUMNS = [
    "FL_DATE",
    "OP_CARRIER",
    "OP_CARRIER_FL_NUM",
    "ORIGIN",
    "DEST",
    "CRS_DEP_TIME",
    "DEP_TIME",
    "DEP_DELAY",
    "TAXI_OUT",
    "WHEELS_OFF",
    "WHEELS_ON",
    "TAXI_IN",
    "CRS_ARR_TIME",
    "ARR_TIME",
    "ARR_DELAY",
    "CANCELLED",
    "CANCELLATION_CODE",
    "DIVERTED",
    "CRS_ELAPSED_TIME",
    "ACTUAL_ELAPSED_TIME",
    "AIR_TIME",
    "DISTANCE",
    "CARRIER_DELAY",
    "WEATHER_DELAY",
    "NAS_DELAY",
    "SECURITY_DELAY",
    "LATE_AIRCRAFT_DELAY",
    # --- Thêm các cột ID nếu cần phân tích chi tiết hơn ---
    # "ORIGIN_AIRPORT_ID",       # ID sân bay gốc (ổn định qua các năm)
    # "ORIGIN_AIRPORT_SEQ_ID",   # ID duy nhất của sân bay gốc tại thời điểm cụ thể
    # "ORIGIN_CITY_MARKET_ID",   # ID thị trường thành phố gốc
    # "DEST_AIRPORT_ID",         # ID sân bay đích
    # "DEST_AIRPORT_SEQ_ID",     # ID duy nhất của sân bay đích tại thời điểm cụ thể
    # "DEST_CITY_MARKET_ID",     # ID thị trường thành phố đích
]
 
# Thời gian nghỉ giữa các request (giây) để tránh làm quá tải server
SLEEP_TIME = 5
 
# User Agent để giả lập trình duyệt
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Referer': BASE_URL # Thêm Referer có thể hữu ích
}
# --- Kết thúc cấu hình ---
 
def get_form_details(session, url):
    """Lấy các giá trị ẩn cần thiết từ form."""
    try:
        response = session.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status() # Kiểm tra lỗi HTTP
        soup = BeautifulSoup(response.text, 'html.parser')
 
        viewstate = soup.find('input', {'name': '__VIEWSTATE'})
        viewstategenerator = soup.find('input', {'name': '__VIEWSTATEGENERATOR'})
        eventvalidation = soup.find('input', {'name': '__EVENTVALIDATION'})
 
        if not viewstate or not viewstategenerator or not eventvalidation:
             print(f"Cảnh báo: Không tìm thấy đủ các trường ẩn (__VIEWSTATE, __VIEWSTATEGENERATOR, __EVENTVALIDATION) trong HTML từ {url}.")
             print("Script có thể không hoạt động đúng.")
             # In ra một phần HTML để debug nếu cần
             # print(soup.prettify()[:2000])
             return None, None, None
 
        return viewstate.get('value'), viewstategenerator.get('value'), eventvalidation.get('value')
 
    except requests.exceptions.RequestException as e:
        print(f"Lỗi mạng khi lấy chi tiết form từ {url}: {e}")
        return None, None, None
    except Exception as e:
        print(f"Lỗi không xác định khi xử lý form từ {url}: {e}")
        return None, None, None
 
 
def download_file(session, url, data, year, month):
    """Thực hiện POST request để tải file."""
    filename = f"bts_ontime_{year}_{month:02d}.zip"
    filepath = os.path.join(OUTPUT_DIR, filename)
 
    if os.path.exists(filepath):
        print(f"Tệp {filename} đã tồn tại. Bỏ qua.")
        return True
 
    print(f"Đang tải xuống: Năm {year}, Tháng {month:02d}...")
    try:
        # Quan trọng: Gửi POST request đến cùng URL
        response = session.post(url, data=data, headers=HEADERS, stream=True, timeout=600) # Tăng timeout cho tải file lớn
        response.raise_for_status()
 
        # Kiểm tra xem response có phải là file zip không
        content_type = response.headers.get('Content-Type', '').lower()
        content_disposition = response.headers.get('Content-Disposition', '')
 
        # Kiểm tra cả content_type và content_disposition (an toàn hơn)
        is_zip = 'zip' in content_type or 'zip' in content_disposition
 
        if response.status_code == 200 and is_zip:
             # Thử lấy tên file từ header, nếu không được thì dùng tên đã tạo
            fname_match = re.search(r'filename="?([^"]+)"?', content_disposition)
            if fname_match:
                actual_filename = fname_match.group(1)
                # Đôi khi tên file thực tế có thể khác, nhưng chúng ta sẽ lưu với tên chuẩn của mình
                print(f"  Server trả về tên file: {actual_filename}. Lưu thành: {filename}")
            else:
                 print(f"  Không tìm thấy tên file trong header Content-Disposition. Lưu thành: {filename}")
 
 
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk: # filter out keep-alive new chunks
                        f.write(chunk)
            print(f"  Đã lưu thành công: {filepath}")
            return True
        else:
            print(f"  Lỗi: Response không phải là file zip hoặc status code không phải 200.")
            print(f"  Status Code: {response.status_code}")
            print(f"  Content-Type: {content_type}")
            # In ra một phần nội dung để kiểm tra lỗi (nếu là HTML)
            if 'html' in content_type:
                print("  Nội dung trả về (có thể là trang lỗi):")
                print(response.text[:500])
            return False
 
    except requests.exceptions.Timeout:
        print(f"  Lỗi: Hết thời gian chờ khi tải xuống {filename}.")
        if os.path.exists(filepath): # Xóa file tải dở nếu có lỗi timeout
             os.remove(filepath)
        return False
    except requests.exceptions.RequestException as e:
        print(f"  Lỗi mạng khi tải xuống {filename}: {e}")
        if os.path.exists(filepath): # Xóa file tải dở
             os.remove(filepath)
        return False
    except Exception as e:
        print(f"  Lỗi không xác định khi tải/lưu file {filename}: {e}")
        if os.path.exists(filepath): # Xóa file tải dở
             os.remove(filepath)
        return False
 
 
# --- Bắt đầu script ---
if not os.path.exists(OUTPUT_DIR):
    print(f"Tạo thư mục đầu ra: {OUTPUT_DIR}")
    os.makedirs(OUTPUT_DIR)
 
session = requests.Session() # Sử dụng Session để duy trì cookies
 
# Lấy các giá trị form ban đầu
print("Đang lấy thông tin form ban đầu...")
initial_viewstate, initial_viewstategen, initial_eventvalid = get_form_details(session, BASE_URL)
 
if not initial_viewstate:
    print("Không thể lấy thông tin form cần thiết. Thoát.")
    exit()
 
print("Lấy thông tin form thành công.")
 
current_year = datetime.now().year
current_month = datetime.now().month
 
for year in range(START_YEAR, END_YEAR + 1):
    for month in range(1, 13):
        # Kiểm tra nếu đã vượt quá tháng/năm hiện tại (vì dữ liệu tương lai chưa có)
        # Trang web này có vẻ cung cấp dữ liệu dự báo/đã có cho các tháng tương lai trong năm hiện tại,
        # nên chúng ta sẽ không dừng lại ở tháng hiện tại mà tải hết đến END_YEAR / tháng 12.
        # if year > current_year or (year == current_year and month > current_month):
        #     print(f"Đã đạt đến {current_month}/{current_year}. Dừng lại.")
        #     break # Thoát vòng lặp tháng
 
        # --- Chuẩn bị dữ liệu payload cho POST request ---
        # Luôn sử dụng lại các giá trị ẩn lấy được ban đầu là đủ trong nhiều trường hợp với session
        # Nếu gặp lỗi, có thể cần lấy lại các giá trị này trước mỗi POST
        payload = {
            '__EVENTTARGET': '',
            '__EVENTARGUMENT': '',
            '__LASTFOCUS': '',
            '__VIEWSTATE': initial_viewstate,
            '__VIEWSTATEGENERATOR': initial_viewstategen,
            '__EVENTVALIDATION': initial_eventvalid,
            'txtSearch': '', # Trường tìm kiếm, để trống
            'cboGeography': 'All', # Giữ nguyên 'All' vì đã lọc theo năm/tháng
            'cboYear': str(year),
            'cboPeriod': str(month),
            'chkDownloadZip': 'on', # RẤT QUAN TRỌNG: Chọn tải file ZIP
            # Các checkbox khác không cần thiết cho việc tải dữ liệu chính
            'chkAllVars': 'off',
            'chkAllGroups': 'off',
            'chkshowNull': 'off', # Mặc định không chọn
            'chkMergeSub': 'off', # Mặc định không chọn
            'chkDocument': 'off', # Mặc định không chọn
            'chkTermDef': 'off', # Mặc định không chọn
            'btnDownload': 'Download' # Giá trị của nút Download
        }
 
        # Thêm các cột cần chọn vào payload
        for col in REQUIRED_COLUMNS:
            payload[col] = 'on' # Đánh dấu là đã chọn
 
        # Thực hiện tải file
        success = download_file(session, BASE_URL, payload, year, month)
 
        if not success:
            print(f"!!! Không thể tải xuống tệp cho {month:02d}/{year}. Thử lại hoặc kiểm tra lỗi.")
            # Bạn có thể thêm logic thử lại ở đây nếu muốn
 
        # Nghỉ giữa các request
        print(f"  Nghỉ {SLEEP_TIME} giây...")
        time.sleep(SLEEP_TIME)
 
    # if year > current_year or (year == current_year and month > current_month):
    #     break # Thoát vòng lặp năm nếu đã dừng ở vòng lặp tháng
 
print("\nQuá trình tải xuống hoàn tất!")