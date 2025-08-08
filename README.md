# ClickHouse Enterprise Data Warehouse Project

## Tổng quan

Enterprise Data Warehouse solution sử dụng ClickHouse, Apache Airflow, dbt, và Kafka với phương pháp Data Vault 2.0 cho xử lý dữ liệu thời gian thực và batch processing để tạo ra nguồn dữ liệu thống nhất cho các báo cáo và phân tích kinh doanh. Dự án này bao gồm việc xử lý dữ liệu từ nhiều nguồn khác nhau, áp dụng các business rules thông qua Excel dynamic filters, và cung cấp khả năng mở rộng và hiệu suất cao. Từ nguồn dữ liệu thống nhất EDW đó có thể tạo ra các data marts cho các deal khác nhau phục vụ cho việc phân tích nhanh.

### Vấn đề đặt ra:
#### Các thách thức trong quy trình xử lý dữ liệu hiện tại:
1. **Quy trình thủ công và rời rạc**: Mỗi "deal" mới đòi hỏi Data Analyst (DA) phải thực hiện lại toàn bộ quy trình từ đầu: dịch yêu cầu, viết SQL, trích xuất, xử lý và tải dữ liệu. Quy trình này tốn thời gian, dễ phát sinh lỗi và không có tính tái sử dụng.
2. **Dữ liệu bị phân mảnh và trùng lặp (Data Silos & Duplication)**: Việc mỗi deal có một data mart riêng dẫn đến tình trạng các dữ liệu chung (như thông tin khách hàng, sản phẩm) bị sao chép ở nhiều nơi. Điều này không chỉ lãng phí không gian lưu trữ mà còn tạo ra sự thiếu nhất quán.
3. **Tải dữ liệu không hiệu quả**: Việc phải tải lại toàn bộ dữ liệu mỗi khi cần cập nhật là cực kỳ thiếu hiệu quả, làm tăng gánh nặng cho hệ thống nguồn và kéo dài thời gian xử lý.
4. **Khó quản lý sự thay đổi của dữ liệu (Slowly Changing Dimensions - SCD)**: Do không có mô hình dữ liệu chuẩn và thiếu các dấu mốc thời gian (timestamp), việc theo dõi lịch sử thay đổi của dữ liệu (ví dụ: địa chỉ khách hàng thay đổi) là bất khả thi. Điều này làm giảm nghiêm trọng độ chính xác và tin cậy của dữ liệu theo thời gian.
5. **Thiếu linh hoạt khi yêu cầu thay đổi**: Khi khách hàng muốn thêm một thuộc tính hay góc nhìn mới, việc thay đổi cấu trúc (schema) và logic tải dữ liệu trở thành một bài toán phức tạp, có thể phá vỡ các quy trình hiện có.
6. **Chất lượng dữ liệu kém và không nhất quán**: Khi mỗi DA có một cách xử lý, làm sạch dữ liệu riêng, sẽ không có một tiêu chuẩn chung nào được áp dụng. Điều này dẫn đến dữ liệu không đồng nhất, chứa nhiều lỗi và không đáng tin cậy để ra quyết định kinh doanh.
7. **Không có "Nguồn sự thật duy nhất" (Single Source of Truth)**: Vì dữ liệu bị trùng lặp ở nhiều nơi, sẽ không có nơi nào được coi là nguồn dữ liệu chính xác và đầy đủ nhất. Khi cần báo cáo, các phòng ban khác nhau có thể đưa ra những con số khác nhau cho cùng một chỉ số, gây ra tranh cãi và mất lòng tin vào dữ liệu.
8. **Rủi ro về bảo mật và quản trị dữ liệu**: Một hệ thống phân mảnh, không có quy trình tập trung sẽ rất khó để kiểm soát truy cập, phân quyền và đảm bảo dữ liệu nhạy cảm được bảo vệ đúng cách. Việc quản lý dữ liệu sai cách có thể dẫn đến mất mát hoặc rò rỉ thông tin quan trọng.
9. **Hiệu năng truy vấn thấp**: Việc truy vấn trên các data mart riêng lẻ với dữ liệu trùng lặp và cấu trúc không được tối ưu hóa sẽ rất chậm chạp, đặc biệt với các phân tích phức tạp. Điều này làm chậm quá trình ra quyết định.
10. **Khó khăn trong việc mở rộng (Lack of Scalability)**: Quy trình thủ công này có thể tạm ổn với vài deal, nhưng sẽ hoàn toàn sụp đổ khi số lượng deal, lượng dữ liệu hoặc độ phức tạp của yêu cầu tăng lên. Hệ thống sẽ không thể đáp ứng được nhu cầu phát triển của doanh nghiệp.
11. **Tăng chi phí ẩn và nợ kỹ thuật (Technical Debt)**: Chi phí không chỉ nằm ở thời gian làm việc của nhân sự mà còn ở các chi phí ẩn như sửa lỗi, xử lý dữ liệu sai lệch và những tổn thất do quyết định kinh doanh sai lầm. Mỗi "deal" được xử lý theo kiểu này sẽ làm gia tăng "nợ kỹ thuật", khiến cho việc xây dựng một hệ thống chuẩn chỉnh trong tương lai càng thêm khó khăn và tốn kém.

### Để giải quyết những vấn đề này, dự án sẽ áp dụng các giải pháp sau:

Xây dựng một Enterprise Data Warehouse (EDW) tập trung với các mục tiêu:
- Tự động hóa quy trình tích hợp dữ liệu từ nhiều nguồn
- Quản lý “deal” linh hoạt thông qua cấu hình Excel động
- Đồng bộ dữ liệu thời gian thực với CDC
- Đảm bảo chất lượng và tính nhất quán của dữ liệu
- Theo dõi lịch sử thay đổi với SCD Type 4
- Tối ưu hiệu năng truy vấn và báo cáo

# Phần 1: Giải thích Luồng của Dự án

## 1.1. Kiến trúc Tổng thể

### **1. **Tổng quan luồng chính****

Hệ thống hoạt động theo mô hình pipeline kép với hai luồng dữ liệu song song:
- **Initial Load**: Xử lý dữ liệu lịch sử lần đầu tiên
- **CDC (Change Data Capture)**: Xử lý các thay đổi liên tục theo thời gian thực.

Cả hai luồng đều xuất phát từ ClickHouse Source Database và kết thúc tại Data Vault 2.0 EDW trên ClickHouse đích.

### **2. Luồng Initial Load (Tải dữ liệu ban đầu)**

```
1. Excel Filter Detection
   └── ExcelFilterProcessor đọc deal_filters.xlsx
   └── Phát hiện deals mới (chưa có trong hub_deal)

2. Data Extraction
   └── ClickHouseManager extract data từ Source
   └── Apply filters cho từng deal (1 record có thể thuộc nhiều deals)
   └── Generate Data Vault hash keys

3. Parallel Loading
   └── Split data thành batches
   └── ThreadPoolExecutor parallel insert vào staging
   └── Data Integrity validation & deduplication

4. dbt Transformation
   └── Staging normalization
   └── Hub tables (business keys)
   └── Link tables (relationships)
   └── Satellite tables (attributes + history)
```


**Bước 1: Kích hoạt thủ công**

- Quá trình bắt đầu khi được kích hoạt thủ công trong Apache Airflow
- Hệ thống sẽ thực hiện query đến Hub Deal trong EDW để lấy danh sách các deal đã tồn tại

**Bước 2: Phát hiện deal mới**

- So sánh danh sách deal hiện có với tên các sheet trong file Excel cấu hình
- Nếu phát hiện có sheet mới (tương ứng deal mới), hệ thống sẽ tiếp tục xử lý
- Nếu không có deal mới, hệ thống dừng và ghi log “Không phát sinh deal mới”

**Bước 3: Trích xuất dữ liệu lịch sử**
- Với mỗi deal mới được phát hiện, hệ thống đọc bộ lọc tương ứng từ sheet Excel
- Dịch các điều kiện lọc thành câu lệnh SQL
- Thực hiện truy vấn toàn bộ dữ liệu lịch sử từ ClickHouse Source thỏa mãn điều kiện

**Bước 4: Xử lý và phân loại dữ liệu**
- Dữ liệu được trích xuất sẽ đi qua bộ lọc Excel để xác định thuộc deal nào
- Một bản ghi có thể thuộc nhiều deal cùng lúc
- Gắn metadata `deal_name` cho mỗi bản ghi

**Bước 5: Chuẩn hóa tại Staging Layer**
- Dữ liệu được normalize để tránh duplicate thông tin sản phẩm
- Thay vì lưu trùng lặp toàn bộ thông tin sản phẩm cho mỗi deal, hệ thống tách biệt:
- Thông tin sản phẩm chỉ lưu một lần
- Quan hệ sản phẩm-deal được lưu riêng

**Bước 6: Tải vào EDW**
- Dữ liệu được chia thành batches nhỏ
- Sử dụng multi-threading và multi-processing để tải song song vào các bảng Data Vault 2.0:
- Hub Product: Khóa nghiệp vụ của sản phẩm
- Hub Deal: Khóa nghiệp vụ của deal
- Link Product-Deal: Quan hệ nhiều-nhiều
- Satellites: Thuộc tính chi tiết

### **3. Luồng CDC (Đồng bộ thay đổi liên tục)**
```
1. Change Capture
   └── User updates data trong ClickHouse Source
   └── Materialized View capture thay đổi
   └── Insert vào product_changes_log

2. Debezium Processing
   └── JDBC Connector poll changes mỗi 5s
   └── Generate CDC events với op: c/u/d
   └── Publish lên Kafka topic

3. Kafka Streaming
   └── CDC events được partition theo product_id
   └── Airflow KafkaMessageSensor detect new messages
   └── Trigger CDC processing DAG

4. CDC Processing
   └── Consume messages từ Kafka
   └── Apply Excel filters (dynamic business rules)
   └── Data validation & duplicate detection
   └── Parallel insert vào EDW staging

5. Incremental Transformation
   └── dbt incremental models
   └── Update Hub/Link/Satellite tables
   └── Maintain data lineage & audit trail
```
**Bước 1: Ghi lại thay đổi**
- Materialized View trong ClickHouse Source tự động ghi lại mọi thay đổi (INSERT, UPDATE, DELETE) vào một bảng log riêng
- Mỗi thay đổi được đánh dấu thời gian và loại thao tác

**Bước 2: Stream qua Kafka**
- Debezium JDBC Connector liên tục đọc từ bảng log
- Các thay đổi được đẩy vào Kafka topics theo thời gian thực
- Mỗi message chứa đầy đủ thông tin về bản ghi cũ và mới

**Bước 3: Xử lý real-time filtering**
- Kafka Consumer nhận các change events
- Mỗi thay đổi được đưa qua bộ lọc Excel để xác định thuộc deal nào
- Chỉ những thay đổi thuộc ít nhất một deal mới được xử lý tiếp

**Bước 4: Cập nhật incremental**
- Thay đổi được áp dụng vào Staging Layer với logic normalize tương tự Initial Load
- Dữ liệu được cập nhật vào EDW theo nguyên tắc SCD Type 4:
- Bảng `_CURRENT`: Cập nhật trạng thái hiện tại
- Bảng `_HISTORY`: Thêm bản ghi lịch sử mới

### **4. Xử lý bộ lọc động từ Excel**
Cấu trúc file Excel:
- Mỗi deal tương ứng với một sheet riêng biệt
- Tên sheet chính là tên deal (ví dụ: “Hapas”, “Obagi”, “Loreal”)
- Trong mỗi sheet chứa các điều kiện lọc định nghĩa deal đó
Quy trình áp dụng filter:
- Hệ thống đọc toàn bộ các sheet trong file Excel
- Với mỗi bản ghi dữ liệu (dù từ Initial Load hay CDC), hệ thống sẽ:
- Kiểm tra lần lượt điều kiện của từng deal
- Xác định bản ghi thuộc deal nào (có thể nhiều deal)
- Gắn các `deal_name` tương ứng vào metadata

### 5. Giai đoạn Transformation với dbt
Sau khi dữ liệu đã có trong Raw Data Vault:
- dbt Core thực hiện các transformation để tạo Business Vault
- Áp dụng các business rules và tính toán các metrics
- Tạo các Data Marts phục vụ báo cáo và phân tích
- Tối ưu hiệu năng truy vấn bằng cách tạo các bảng tổng hợp (aggregated tables)
### 6. Đảm bảo tính toàn vẹn dữ liệu
Giữa Initial Load và CDC:
- Hệ thống đảm bảo không có khoảng trống thời gian giữa việc kết thúc Initial Load và bắt đầu CDC
- Sử dụng timestamp để đánh dấu thời điểm chuyển giao
- Có cơ chế kiểm tra và xử lý duplicate nếu có overlap
Trong quá trình CDC:
- Mỗi change event được xử lý đúng một lần (exactly-once processing)
- Có retry mechanism cho các lỗi tạm thời
- Monitoring để phát hiện sớm các vấn đề về data quality

Luồng dữ liệu này đảm bảo EDW luôn có dữ liệu mới nhất, đồng nhất và đáng tin cậy, đồng thời hỗ trợ việc thêm deal mới một cách linh hoạt mà không ảnh hưởng đến các deal hiện có.
## 1.2. Ưu nhược điểm của kiến trúc này
### **Ưu điểm**
### **1. Kiến trúc và Thiết kế**

**Data Vault 2.0 trên ClickHouse:**

- **Tính linh hoạt cao**: Dễ dàng thêm deal mới mà không ảnh hưởng đến cấu trúc hiện có[1][2]
- **Khả năng mở rộng tuyệt vời**: Hỗ trợ tích hợp nhiều nguồn dữ liệu đồng thời mà không cần thiết kế lại[1][2]
- **Auditability mạnh**: Theo dõi đầy đủ lịch sử thay đổi với SCD Type 4, đáp ứng yêu cầu compliance[1][2]
- **Hiệu năng truy vấn cao**: ClickHouse với kiến trúc columnar storage mang lại tốc độ xử lý vượt trội cho OLAP[3][4]

**Pipeline Architecture:**

- **Real-time sync**: CDC với Kafka + Debezium đảm bảo dữ liệu luôn được cập nhật theo thời gian thực[5][6]
- **Parallel processing**: Multi-threading và multi-processing tối ưu hiệu năng tải dữ liệu[4]
- **Fault tolerance**: Cơ chế retry và exactly-once processing đảm bảo tính toàn vẹn dữ liệu

### **2. Quản lý và Vận hành**

**Dynamic Configuration:**

- **Excel-based filtering**: Cho phép business users tự cấu hình deal mới mà không cần developer[7]
- **No hardcoding**: Logic filter linh hoạt, dễ thay đổi theo yêu cầu kinh doanh
- **Self-service capability**: Giảm dependency vào IT team cho việc thêm deal mới

**Orchestration với Airflow:**

- **Visual monitoring**: Giao diện web trực quan để theo dõi pipeline status[8]
- **Scheduling flexibility**: Hỗ trợ nhiều pattern scheduling phức tạp[8]
- **Error handling**: Retry mechanism và alerting tự động khi có lỗi

### **3. Data Quality và Consistency**

**Normalized Staging Layer:**

- **Loại bỏ data duplication**: Tối ưu không gian lưu trữ và đảm bảo Single Source of Truth
- **Data integrity**: Referential integrity được duy trì qua các bảng Hub-Link-Satellite
- **Historical tracking**: SCD Type 4 cho phép truy vết mọi thay đổi theo thời gian

### **Nhược điểm**
### **1. Thiếu Lớp Presentation** 

**Accessibility Issues:**

- **No simplified views**: Thiếu các views hoặc data marts đơn giản hóa cho reporting và analytics
- **Complex queries**: Business users phải viết SQL phức tạp để truy vấn dữ liệu


### **2. Thiếu Tích hợp Business Rules và Data Enrichment** 

**Limited Business Logic:**

- **Raw data only**: Hệ thống chỉ tập trung vào việc ingest và store raw data, chưa áp dụng business transformations

**Data Enrichment Gaps:**

- **Enrich data**: Có thể tích hợp với repository sau để enrich dữ liệu: https://github.com/hnacud1106/using_PydanticAI_and_LLM_to_clean_data

### **3. Thiếu Monitoring và Observability**

**Operational Blindness:**

- **No comprehensive monitoring**: Thiếu monitoring cho data quality, pipeline performance, và system health
- **Manual issue detection**: Phải manually check logs để phát hiện problems
- **No proactive alerting**: Không có early warning system cho data anomalies hoặc system failures

**Performance Visibility:**

- **No metrics tracking**: Không track throughput, latency, error rates của pipeline
- **Resource utilization**: Không monitor CPU, memory, disk usage across components
- **Data lineage visibility**: Khó trace data flow và identify bottlenecks

### **4. Operational Challenges**

**Excel Dependency:**
- **Single point of failure**: Nếu Excel file bị corrupt hoặc misconfigured có thể ảnh hưởng toàn bộ pipeline
- **Version control**: Khó kiểm soát changes history của Excel configuration
- **Scalability limits**: Khi số lượng deals tăng lên hàng trăm, Excel có thể không còn practical

### **5. Governance và Maintenance**

**Business Dependencies:**

- **Excel expertise required**: Business users cần hiểu cách config Excel filters properly
- **Data lineage tracking**: Với dynamic filtering, khó trace data lineage cho specific records
- **Compliance challenges**: Dynamic rules có thể khó audit và explain cho regulators




## 1.3. Các Thành phần Chính

### **ClickHouse Source (Staging Database)**
- **Mục đích**: Lưu trữ dữ liệu sản phẩm e-commerce thô
- **Chức năng**: 
  - Chứa bảng `products` với thông tin sản phẩm, giá, doanh thu
  - Materialized View để capture changes cho CDC
  - Bảng `product_changes_log` để ghi lại mọi thay đổi

### **Excel Dynamic Filters**
- **Mục đích**: Định nghĩa business rules cho các deals
- **Cách hoạt động**:
  - Mỗi sheet Excel = 1 deal (Hapas, Obagi, Loreal...)
  - Chứa các điều kiện filter (brand, category, price...)
  - Hỗ trợ operators: EQUALS, CONTAINS, GREATER_THAN, BETWEEN, IN
  - Non-technical users có thể modify business logic

### **Kafka + Debezium CDC**
- **Mục đích**: Real-time change data capture
- **Luồng hoạt động**:
  - Debezium JDBC Connector poll ClickHouse Source mỗi 5 giây
  - Đọc dữ liệu từ `product_changes_log` table
  - Publish CDC events lên Kafka topic `edw_cdc.product_changes_log`
  - Đảm bảo exactly-once delivery

### **Apache Airflow Orchestration**
- **Initial Load DAG**: Xử lý load dữ liệu lần đầu
- **CDC Processing DAG**: Xử lý real-time changes từ Kafka
- **Parallel Processing**: Multi-threading với ThreadPoolExecutor
- **Error Handling**: Retry logic, watermark management

### **dbt Data Transformation**
- **Data Vault 2.0 Modeling**:
  - **Hubs**: Business keys (hub_product, hub_deal)
  - **Links**: Relationships (link_product_deal)
  - **Satellites**: Attributes with history (sat_product_current, sat_product_history)
- **SCD Type 4**: Full historical tracking
- **Incremental Models**: Efficient processing cho CDC data

### **ClickHouse EDW (Data Warehouse)**
- **Mục đích**: Final data warehouse với Data Vault structure
- **Tối ưu**: Partitioning, compression, query performance
- **Tables**: Hub, Link, Satellite tables theo Data Vault 2.0

# Phần 2: Cách Triển khai Dự án

## 2.1. Cài đặt Môi trường

### **Bước 1: Clone Repository**
```bash
# Clone project
git clone 
cd clickhouse-edw-project
```

### **Bước 2: Setup Python Environment**
```bash
# Tạo virtual environment
python3 -m venv venv

# Activate environment
source venv/bin/activate  # Linux/macOS
# hoặc venv\Scripts\activate  # Windows

# Upgrade pip
pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt
```

### **Bước 3: Configuration**
```bash
# Copy và customize environment file
cp .env.example .env
```

## 2.3. Triển khai Infrastructure

### **One-Command Deployment**
```bash
# Full infrastructure deployment
chmod +x scripts/*.sh
./scripts/setup_infrastructure.sh
```
### **Service Access**
- **Airflow UI**: http://localhost:8080 (admin/admin)
- **ClickHouse Source**: http://localhost:8123
- **ClickHouse EDW**: http://localhost:8124
- **Debezium Connect**: http://localhost:8083/connectors

## 2.5. Chạy Pipeline

### **Initial Load Pipeline**

**Step 1: Access Airflow**
```bash
# Open browser
open http://localhost:8080

# Login credentials
Username: admin
Password: admin
```

**Step 2: Trigger Initial Load**
1. Tìm DAG `initial_load_pipeline`
2. Trigger DAG

**Step 3: Verify Results**
```bash
# Check staging data
docker exec clickhouse-edw clickhouse-client --query="
SELECT deal_name, count(*) 
FROM edw.stg_products 
GROUP BY deal_name
"

# Check Data Vault tables
docker exec clickhouse-edw clickhouse-client --query="
SELECT 'hub_product', count(*) FROM edw.hub_product
UNION ALL
SELECT 'hub_deal', count(*) FROM edw.hub_deal  
UNION ALL
SELECT 'link_product_deal', count(*) FROM edw.link_product_deal
"
```

### **CDC Pipeline Testing**

**Step 1: Enable CDC DAG**
1. Trong Airflow UI, enable `cdc_processing_pipeline`
2. DAG sẽ tự động chạy mỗi 5 phút

**Step 2: Test Real-time Changes**
```bash
# Insert new product
docker exec clickhouse-source clickhouse-client --query="
INSERT INTO staging.products VALUES 
(999, 'Test CDC Product', 'Test Description', 'Skincare', 'Hapas', 4.0, 'test.jpg', 'Test Store', 'https://test.com', 1000000.00, '2025-01', 100000.00, 10, now(), now())
"

# Update existing product
docker exec clickhouse-source clickhouse-client --query="
ALTER TABLE staging.products UPDATE price = 300000.00 WHERE product_base_id = 1
"


# Check CDC logs
docker exec clickhouse-source clickhouse-client --query="
SELECT operation_type, count(*) 
FROM staging.product_changes_log 
GROUP BY operation_type
"
```

## 2.6. Configuration Management

### **Excel Filters Management**

**File Location:** `configs/deal_filters.xlsx`

**Format:**
```
Sheet "Hapas":
| column_name | operator      | value    | logical_operator |
|-------------|---------------|----------|------------------|
| brand       | CONTAINS      | Hapas    | AND              |
| category    | EQUALS        | Skincare | AND              |
| price       | GREATER_THAN  | 100000   | AND              |

Sheet "Obagi":
| column_name | operator | value          | logical_operator |
|-------------|----------|----------------|------------------|
| brand       | CONTAINS | Obagi          | AND              |
| category    | IN       | Skincare,Beauty| AND              |
```

**Thêm Deal Mới:**
1. Tạo sheet mới trong Excel với tên deal
2. Định nghĩa filter conditions
3. Save file Excel
4. Trigger `initial_load_pipeline` để process deal mới

