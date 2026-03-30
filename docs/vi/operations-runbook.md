# Sổ tay Vận hành ZeroClaw

Tài liệu này dành cho các operator chịu trách nhiệm duy trì tính sẵn sàng, tình trạng bảo mật và xử lý sự cố.

Cập nhật lần cuối: **2026-03-30**.

## Phạm vi

Dùng tài liệu này cho các tác vụ vận hành day-2:

- khởi động và giám sát runtime
- kiểm tra sức khoẻ và chẩn đoán hệ thống
- triển khai an toàn và rollback
- phân loại và khôi phục sau sự cố

Nếu đây là lần cài đặt đầu tiên, hãy bắt đầu từ [one-click-bootstrap.md](one-click-bootstrap.md).

## Các chế độ Runtime

| Chế độ | Lệnh | Khi nào dùng |
|---|---|---|
| Foreground runtime | `zeroclaw daemon` | gỡ lỗi cục bộ, phiên ngắn |
| Foreground gateway only | `zeroclaw gateway` | kiểm thử webhook endpoint |
| User service | `zeroclaw service install && zeroclaw service start` | runtime được quản lý liên tục bởi operator |

## Checklist Cơ bản cho Operator

1. Xác thực cấu hình:

```bash
zeroclaw status
```

2. Kiểm tra chẩn đoán:

```bash
zeroclaw doctor
zeroclaw channel doctor
```

3. Khởi động runtime:

```bash
zeroclaw daemon
```

4. Để chạy như user session service liên tục:

```bash
zeroclaw service install
zeroclaw service start
zeroclaw service status
```

## Tín hiệu Sức khoẻ và Trạng thái

| Tín hiệu | Lệnh / File | Kỳ vọng |
|---|---|---|
| Tính hợp lệ của config | `zeroclaw doctor` | không có lỗi nghiêm trọng |
| Kết nối channel | `zeroclaw channel doctor` | các channel đã cấu hình đều khoẻ mạnh |
| Tóm tắt runtime | `zeroclaw status` | provider/model/channels như mong đợi |
| Heartbeat/trạng thái daemon | `~/.zeroclaw/daemon_state.json` | file được cập nhật định kỳ |
| Sức khoẻ Context Book | `zeroclaw doctor` + khối `context_book` trong daemon state | instance đã bật hiển thị worker state, connection state, cache freshness và degraded contract nếu có |

## Vận hành Context Book

Dùng mục này khi bật `[context_book]`.

- Cấu hình `context_book.manual_url` hoặc discovery, và luôn giới hạn truy cập outbound bằng `context_book.allowed_hosts`.
- Với endpoint loopback hoặc private, đặt `context_book.allow_private_hosts = true`; nếu không client sẽ từ chối đích trước bước connect/bootstrap.
- Bootstrap secret được đọc từ biến môi trường do `context_book.bootstrap_secret_env_key` chỉ định. Không ghi secret này vào `config.toml` hoặc shell history.
- Daemon sở hữu subscription worker sống lâu. Các lần chạy CLI hoặc tool một lần vẫn có thể đọc/ghi trạng thái Context Book, nhưng không tự động khởi động SSE worker.
- Context và vote lấy về được cache dưới `workspace/context_book/cache.db`; chúng không bị sao chép vào backend memory chuẩn.

Kiểm tra dành cho operator:

```bash
zeroclaw doctor
cat ~/.zeroclaw/daemon_state.json | jq '.context_book'
```

Cần theo dõi:

- `connection_state` không quay lại `connected`
- cache freshness của agents, contexts hoặc votes bị stale
- degraded contract như `read_only`, `no_write`, `no_refresh`, hoặc `disconnect`
- discovery thất bại và cần chuyển sang `manual_url`

## Log và Chẩn đoán

### macOS / Windows (log của service wrapper)

- `~/.zeroclaw/logs/daemon.stdout.log`
- `~/.zeroclaw/logs/daemon.stderr.log`

### Linux (systemd user service)

```bash
journalctl --user -u zeroclaw.service -f
```

## Quy trình Phân loại Sự cố (Fast Path)

1. Chụp trạng thái hệ thống:

```bash
zeroclaw status
zeroclaw doctor
zeroclaw channel doctor
```

2. Kiểm tra trạng thái service:

```bash
zeroclaw service status
```

3. Nếu service không khoẻ, khởi động lại sạch:

```bash
zeroclaw service stop
zeroclaw service start
```

4. Nếu các channel vẫn thất bại, kiểm tra allowlist và thông tin xác thực trong `~/.zeroclaw/config.toml`.

5. Nếu liên quan đến gateway, kiểm tra cài đặt bind/auth (`[gateway]`) và khả năng tiếp cận cục bộ.

6. Nếu liên quan đến Context Book, kiểm tra:
   - `context_book.allowed_hosts` có khớp host đã resolve hay không
   - `context_book.allow_private_hosts = true` với endpoint loopback/private
   - biến môi trường chứa bootstrap secret có tồn tại hay không
   - `zeroclaw doctor` hoặc daemon state có báo `disconnect`, cache stale, hoặc cursor reset lặp lại hay không

## Quy trình Thay đổi An toàn

Trước khi áp dụng thay đổi cấu hình:

1. sao lưu `~/.zeroclaw/config.toml`
2. chỉ áp dụng một thay đổi logic tại một thời điểm
3. chạy `zeroclaw doctor`
4. khởi động lại daemon/service
5. xác minh bằng `status` + `channel doctor`

## Quy trình Rollback

Nếu một lần triển khai gây ra suy giảm hành vi:

1. khôi phục `config.toml` trước đó
2. khởi động lại runtime (`daemon` hoặc `service`)
3. xác nhận khôi phục qua `doctor` và kiểm tra sức khoẻ channel
4. ghi lại nguyên nhân gốc rễ và biện pháp khắc phục sự cố

## Tài liệu Liên quan

- [one-click-bootstrap.md](one-click-bootstrap.md)
- [troubleshooting.md](troubleshooting.md)
- [config-reference.md](config-reference.md)
- [commands-reference.md](commands-reference.md)
