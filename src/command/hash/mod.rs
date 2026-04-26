// TODO: Hash 命令实现计划
//
// 1. HSET key field value [field value ...]   — 设置一个或多个字段
// 2. HGET key field                           — 获取单个字段的值
// 3. HMGET key field [field ...]              — 批量获取多个字段的值
// 4. HDEL key field [field ...]               — 删除一个或多个字段
// 5. HEXISTS key field                        — 判断字段是否存在
// 6. HLEN key                                 — 获取字段数量
// 7. HGETALL key                              — 获取所有字段和值
// 8. HKEYS key / HVALS key                    — 获取所有字段名 / 所有值
// 9. HINCRBY key field increment              — 字段整数自增
// 10. HINCRBYFLOAT key field increment        — 字段浮点数自增
// 11. HSETNX key field value                  — 字段不存在时才设置
pub mod hset;
pub mod hget;