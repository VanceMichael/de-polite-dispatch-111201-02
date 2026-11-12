# 共享出口下·采集配额调度

面向合作资料接收人员的 Python 采集组件。验证任务登记字段并按优先级和截止时间给出当前候选顺序。
输入样本都是本地合成数据，example 域名只表示来源身份，不需联网访问。

## 安装与运行

使用 Python 3.11，依赖仅为标准库，SQLite 随解释器提供，无需安装第三方包。
从项目根目录执行；演示启动真实的回环 HTTP 来源、处理响应、事务入库后读取实际结果。
每次演示使用独立临时目录，结束后清理；通常两秒内结束，不回放历史输出。

```bash
python3 -m compileall -q collector
python3 -m unittest discover -s tests -v
python3 -m collector demo
python3 -m collector dispatch --quotas fixtures/quotas.json
```

常规导入保留在本地数据库中，可重复运行；默认路径已忽略，不会进入版本库。

```bash
python3 -m collector import --input fixtures/sample.json --db var/records.sqlite3
python3 -m collector show --db var/records.sqlite3
python3 -m collector serve --db var/records.sqlite3 --port 8080
```

查询服务监听回环地址，`GET /records` 返回数据库中的记录；按 Ctrl-C 停止。

## 组件与数据

`collector/domain.py` 负责来源格式和领域校验；`storage.py` 保留原始对象、导入凭据和当前记录。
单次导入在同一事务内提交，异常时整体回滚；相同来源和原文只登记一次凭据。
当前记录以领域标识为键，同键再次导入替换当前观察值，不代表保留了完整修订历史。
`transport.py` 提供请求客户端和本地响应服务；服务按路径依次消费响应脚本，并记录实际请求。
`api.py` 提供只读查询入口。`fixtures/sample.json` 是当前导入器支持的输入。
其他附件描述同领域的合成页面、协议或事件，可以由调用方构造本地来源。
当前响应服务是顺序脚本工具，不自动推断认证、文件或表单业务；领域对象提供对应的本地交互。

`collector/dispatch.py` 是登记器之上的配额派发器：全局并发、供应方并发与账号令牌预算同时生效，
域名别名归并到同一供应方共享配额；令牌在领取时仅预留，请求真正发出时才消费，未发出即交还。
实时任务优先，积压任务随等待老化，持续有新任务时仍能获得服务；过期任务带明确原因移出队列。
多个 worker 领取租约，租约到期回收、停机交还，围栏序号拒绝晚到的旧 worker 完成已转交任务；
429 的 Retry-After 只冻结对应配额桶，不阻塞无关供应方；可重试错误退避重排，永久拒绝进入死信。
时钟与请求执行器均可注入，执行器在锁外调用，不靠串行化换正确性；
运行记录解释每次未派发的原因，指标暴露配额利用率、任务等待与重复请求。
`collector/simulate.py` 用手动时钟与脚本执行器在本地回放配额配置和调度事件。
`fixtures/quotas.json` 给出共享预算与调度事件，`dispatch` 命令回放它并输出运行记录与指标。

## 可复现运行

宿主机或容器均不需要网络。镜像准备好以后可以关闭外部网络再运行上述命令。
仓库内不含真实账号、新闻正文或人员资料；不要将生产凭据写入样本或日志。
