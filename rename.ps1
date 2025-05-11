# Thư mục chứa file cần rename
$folder = "E:\PythonProject\bigdata\data"

# Lọc và xử lý file
Get-ChildItem -Path $folder -Filter "On_Time_Reporting_Carrier_On_Time_Performance_*.csv" | ForEach-Object {
    $oldName = $_.Name

    # Match cuối tên file theo định dạng _YYYY_M.csv
    if ($oldName -match "_(\d{4}_\d{1,2})\.csv$") {
        $newName = "$($matches[1]).csv"
        $newPath = Join-Path $folder $newName

        Rename-Item -Path $_.FullName -NewName $newName
        Write-Host "✅ Đổi tên: $oldName → $newName"
    } else {
        Write-Host "⚠️ Bỏ qua: $oldName (không khớp định dạng)"
    }
}
