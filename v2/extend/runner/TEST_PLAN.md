# 改密 10w 输入构造方案

## 1. 资源限制定义

来源：[runner.go#L504-L604](file:///c:/Users/Administrator/.projects/opensources/machinery/v2/extend/runner/runner.go#L504-L604)

| 资源标识 | 作用域 | 默认值 | 说明 |
|---------|--------|--------|------|
| `ResourceExec` | **全局** | `Concurrency/2+1 ≈ 65` | 所有 DB 和 RDP 共享的全局执行槽 |
| `ResourceSshExec` | 全局 | 128 | SSH 执行槽（仅 SSH 重试时生效） |
| `ResourceTelnetExec` | 全局 | 128 | Telnet 执行槽（仅重试时生效） |
| `ResourceRdpScriptExec` | **全局** | 64 | RDP Script 执行槽，与 `ResourceExec` 双重限制 |
| `ResourceDbSqlshellExec` | 全局 | 128 | DB SQL 执行槽，与 `ResourceExec` 双重限制 |
| `ResourceSshPerAsset` | **单资产** | `MaxStartups ≈ 10` | 同一 SSH 资产的并发上限 |
| `ResourceRDPWinrmPerAsset` | **单资产** | `WinrmMaxStartups ≈ 5` | 同一 RDP 资产 WinRM 并发上限 |
| `ResourceRdpScriptPerAsset` | **单资产** | `RdpMaxStartups ≈ 3` | 同一 RDP 资产 Script 并发上限 |
| `ResourceDbPerAsset` | **单资产** | **2**（硬编码） | 同一 DB 资产的并发上限 |
| `ResourceWebRemoteClient` | 全局 | `RdpMaxStartups` | Web 改密客户端 |
| Runner goroutine 池 | 全局 | `Concurrency = 128` | Runner 最大同时处理 goroutine 数 |

### 并发限制规则

```
一个账号最终能执行的条件 = 该账号所需的每种 Resource 当前占用数 < 限制值
```

每个服务的资源依赖链：

```
SSH (默认无重试): goroutine(128) ∩ sshPerAsset(10/资产)
SSH (重试时):     goroutine(128) ∩ exec(65) ∩ sshExec(128) ∩ sshPerAsset(10/资产)

RDP (WinRM):     goroutine(128) ∩ winrmPerAsset(5/资产)
RDP (Script):    goroutine(128) ∩ exec(65) ∩ rdpScriptExec(64) ∩ rdpScriptPerAsset(3/资产)

DB (默认):       goroutine(128) ∩ exec(65) ∩ dbSqlshellExec(128) ∩ dbPerAsset(2/资产)
```

---

## 2. 10w 数据规模定义

### 2.1 总量分布

| 服务 | 账号数 | 占比 | 资产数 | 每资产账号数 | 改密计划数 | 每计划账号数 |
|------|--------|------|--------|-------------|-----------|-------------|
| SSH | 70,000 | 70% | 350 | 200 | 35 | 2,000 |
| RDP | 10,000 | 10% | 200 | 50 | 10 | 1,000 |
| DB | 20,000 | 20% | 200 | 100 | 10 | 2,000 |
| **合计** | **100,000** | 100% | **750** | — | **55** | — |

### 2.2 DB 子分布（20,000）

| 子类型 | 账号数 | 资产数 | 每资产账号数 |
|--------|--------|--------|-------------|
| MySQL | 8,000 | 80 | 100 |
| Oracle | 6,000 | 60 | 100 |
| PostgreSQL | 6,000 | 60 | 100 |

### 2.3 ID 分配方案

```
SSH 资产:     assetID = 10001 ~ 10350
RDP 资产:     assetID = 20001 ~ 20200
MySQL 资产:   assetID = 30001 ~ 30080
Oracle 资产:  assetID = 30101 ~ 30160
PostgreSQL 资产: assetID = 30201 ~ 30260

SSH 账号:     accountID = 1000001 ~ 1070000
RDP 账号:     accountID = 2000001 ~ 2010000
DB 账号:      accountID = 3000001 ~ 3020000

日志ID:       logID = 1 ~ 100000 (连续)
任务ID:       taskID = 1 ~ 55 (每计划一个)
计划ID:       planID = 1 ~ 55
```

### 2.4 主机地址分配

每资产的 Host IP 由 `baseIP + assetIndex` 决定，确保 `AssetSameID()` 产生的 hash 值不同：

```
SSH 资产 IP:    10.0.0.101 ~ 10.0.0.450   (350个)
RDP 资产 IP:    10.1.0.101 ~ 10.1.0.300   (200个)
MySQL 资产 IP:  10.2.0.101 ~ 10.2.0.180   (80个)
Oracle 资产 IP: 10.2.1.101 ~ 10.2.1.160   (60个)
PG 资产 IP:     10.2.2.101 ~ 10.2.2.160   (60个)
```

---

## 3. 改密耗时模型

每个账号的改密耗时在所属服务的区间内均匀随机分布。Worker 侧 mock 时按 `rand(serviceMin, serviceMax)` sleep 后返回成功。

| 服务 | 最小耗时 | 最大耗时 | 均值 | 分布 |
|------|---------|---------|------|------|
| SSH | 5s | 30s | ~17.5s | uniform |
| RDP (WinRM) | 30s | 90s | ~60s | uniform |
| RDP (Script) | 60s | 180s | ~120s | uniform |
| MySQL | 10s | 45s | ~27.5s | uniform |
| Oracle | 15s | 60s | ~37.5s | uniform |
| PostgreSQL | 10s | 40s | ~25s | uniform |

### 预期总执行时间（单 Runner 串行消费）

各服务独立计算（假设同时消费不互相竞争资源）：

| 服务 | 账号数 | 有效全局并发 | 平均耗时 | 理论耗时 |
|------|--------|-------------|---------|---------|
| SSH | 70,000 | 128 (goroutine池, 无exec限制) | 17.5s | 70,000/128×17.5s ≈ **9,570s ≈ 2.7h** |
| RDP | 10,000 | 64 (exec∩rdpScriptExec) | 120s | 10,000/64×120s ≈ **18,750s ≈ 5.2h** |
| DB | 20,000 | 65 (exec∩dbSqlshellExec) | 30s | 20,000/65×30s ≈ **9,231s ≈ 2.6h** |

**混合竞争时**（所有服务同时注入同一个 Runner）：
- `ResourceExec=65` 被 RDP 和 DB 共享
- SSH 不占 exec，直接占 goroutine 池
- RDP 约分 5 个 exec 槽，DB 约分 15 个 exec 槽（剩余 45 个空置）
- SSH 占 goroutine 128 - 20 = 108
- 期望耗时 ≈ max(70,000/108×17.5s, 10,000/5×120s, 20,000/15×30s)
  ≈ max(11,343s, 240,000s, 40,000s)
  ≈ **240,000s ≈ 66.7h（RDP 为瓶颈）**

> **注意**：RDP 数量少（10,000）但耗时长（120s），且 `rdpScriptExec=64` 成为全局瓶颈。
> 要缩短时间可增加 RDP 资产数或单独测试 RDP 场景。

---

## 4. 每个输入的 Labels 产出

来源：[runner.go#L304-L346](file:///c:/Users/Administrator/.projects/opensources/machinery/v2/extend/runner/runner.go#L304-L346)

> **重要**：runner.go 的资源限制逻辑使用 `label.Value` 作为计数 key（见 [runner.go#L333-L342](file:///c:/Users/Administrator/.projects/opensources/machinery/v2/extend/runner/runner.go#L333-L342)）。
> 因此 Label 的 `Value` 必须全局唯一标识该资源实例。

### 4.1 SSH 输入（无重试，默认）

```
labels = [
  {Name: "sshPerAsset", Value: "sshPerAsset" + sha256("ssh://10.0.0.101")}
]
```

`AssetSameID()` = `sha256("ssh://" + host)` → hex 编码 64 字符。

### 4.2 RDP 输入（Script 隧道，RetryInfos.Enabled=true）

```
labels = [
  {Name: "exec",              Value: "exec"},
  {Name: "rdpScriptExec",     Value: "rdpScriptExec"},
  {Name: "rdpScriptPerAsset", Value: "rdpScriptPerAsset" + sha256("rdp://10.1.0.101")}
]
```

`AssetSameID()` = `sha256(GetMajorService().Name + "://" + host)` → `sha256("rdp://10.1.0.101")`。

> 注意：RDP WinRM 隧道的 labels 不同，只产 `{winrmPerAsset+hash}`

### 4.3 DB 输入（default 分支）

```
labels = [
  {Name: "exec",          Value: "exec"},
  {Name: "dbSqlshellExec", Value: "dbSqlshellExec"},
  {Name: "dbPerAsset",    Value: "dbPerAsset" + sha256("mysql://10.2.0.101")}
]
```

`AssetSameID()` = `sha256("mysql://" + host)`，Oracle 则是 `sha256("oracle://" + host)`。

### 4.4 标签并发约束汇总

每个 Label 的 `Name` 对应 [runner.go#L504-L604](file:///c:/Users/Administrator/.projects/opensources/machinery/v2/extend/runner/runner.go#L504-L604) 中的 `ResourceLimits` map key：
- 相同 `Value` 的 Label 共享同一个资源计数（runner.go 使用 `label.Value` 作为 references map key）
- 全局资源的 `Value` 等于 `Name`（如 `"exec"`），因此所有相同全局资源的 Label 共享计数
- PerAsset 资源的 `Value` 包含资产标识（如 `"dbPerAsset" + hash`），因此同一资产的不同账号共享计数

**关键约束对应表**：

| 输入服务 | Label Name | Label Value 格式 | 对应 Resource | 限制值 | 计数方式 |
|---------|-----------|-----------------|--------------|-------|---------|
| SSH | `sshPerAsset` | `sshPerAsset` + hash | `ResourceSshPerAsset` | MaxStartups(10) | 按 Value（每资产） |
| RDP | `exec` | `exec` | `ResourceExec` | Concurrency/2+1(≈65) | 全局（所有 RDP/DB 共享） |
| RDP | `rdpScriptExec` | `rdpScriptExec` | `ResourceRdpScriptExec` | 64 | 全局 |
| RDP | `rdpScriptPerAsset` | `rdpScriptPerAsset` + hash | `ResourceRdpScriptPerAsset` | RdpMaxStartups(3) | 按 Value（每资产） |
| DB | `exec` | `exec` | `ResourceExec` | Concurrency/2+1(≈65) | 全局（所有 RDP/DB 共享） |
| DB | `dbSqlshellExec` | `dbSqlshellExec` | `ResourceDbSqlshellExec` | 128 | 全局 |
| DB | `dbPerAsset` | `dbPerAsset` + hash | `ResourceDbPerAsset` | 2 | 按 Value（每资产） |

---

## 5. 输入构造代码

### 5.1 核心构造函数

```go
// buildOnePassContext 构造一个带正确 Label 的 PassContext，作为 Runner.Process() 的输入
//
// 生成的 PassContext.Labels() 产出规则见第 4 节。
// 注意：Label 的 Value 必须全局唯一标识资源实例，因为 runner.go 使用 Value 作为计数 key。
func buildOnePassContext(
	ctx context.Context,
	service string,           // "SSH" | "RDP" | "MYSQL" | "ORACLE" | "POSTGRES"
	host string,              // 资产IP，决定 AssetSameID() 的 hash
	username string,          // 账号名
	assetID, accountID, logID int32,
	taskID, planID int32,
) *PassContext {
	packet := &common.ChpassPacket{
		Host:          host,
		Username:      username,
		OldPass:       "placeholder-old-pass", // PreBatch 会从 DB 拉取
		NewPass:       "",                     // 密码策略生成
		AssetID:       assetID,
		AccountID:     accountID,
		ChangeLogID:   logID,
		TaskID:        taskID,
		Action:        common.ActionChange,
		MajorService:  service,
		Update:        true,
		CreateFrom:    common.CreateFromPlan,
		TaskExecVersion: 1,
		SourceIp:      "127.0.0.1",
		ConnectTimeout: 15 * time.Second,
		ExpectTimeout:  30 * time.Second,
		ProcessTimeout: 180 * time.Second,
		BackupInfo: &common.BackupInfo{
			AccountName: username,
			AssetName:   fmt.Sprintf("asset-%d", assetID),
			AssetAddr:   host,
			AccountType: "普通账号",
			PlanName:    fmt.Sprintf("plan-%d", planID),
		},
	}

	// 根据服务类型填充 Services 和 AccountServices
	switch service {
	case "SSH":
		packet.Major = "Linux"
		packet.Minor = "CentOS7"
		packet.Services = []common.ServiceConfig{{Name: "SSH", Port: 22}}
		packet.AccountServices = []string{"SSH"}

	case "RDP":
		packet.Major = "Windows"
		packet.Minor = "WindowsServer2019"
		packet.Services = []common.ServiceConfig{{Name: "RDP", Port: 3389}}
		packet.AccountServices = []string{"RDP"}

	case "MYSQL":
		packet.Major = "Database"
		packet.Services = []common.ServiceConfig{{Name: "MYSQL", Port: 3306, DBName: "mysql"}}
		packet.AccountServices = []string{"MYSQL"}

	case "ORACLE":
		packet.Major = "Database"
		packet.Services = []common.ServiceConfig{{
			Name: "ORACLE", Port: 1521,
			SysRole: "SYSDBA", SID: "ORCL",
		}}
		packet.AccountServices = []string{"ORACLE"}

	case "POSTGRES":
		packet.Major = "Database"
		packet.Services = []common.ServiceConfig{{
			Name: "POSTGRES", Port: 5432, DBName: "postgres",
		}}
		packet.AccountServices = []string{"POSTGRES"}
	}

	// 构造 PassContext — Labels() 由内部自动计算
	passCtx := &PassContext{
		ctx:           ctx,
		Request:       packet,
		UpdateNewPass: true,
		ActionPerm:    PermsChange,
		RetryInfos:    GenRetryFrom(packet), // SSH→{}, RDP→{Enabled,WinRM,Script}, DB→{}
		ProcessLog:    newProccessLog(),
		ElapsedTime:   elapsed.NewElapsed(),
	}

	// 验证 Labels 合理性（调试用）
	// _ = passCtx.Labels()

	return passCtx
}
```

### 5.2 10w 批量生成

```go
// build100kInputs 生成 10w 个 PassContext 输入
func build100kInputs(ctx context.Context) []*PassContext {
	inputs := make([]*PassContext, 0, 100_000)

	// ---- SSH: 70,000 个 (350 资产 × 200 账号) ----
	for i := 0; i < 350; i++ {
		assetID := int32(10001 + i)
		host := fmt.Sprintf("10.0.0.%d", 101+i)
		for j := 0; j < 200; j++ {
			accountID := int32(1_000_001 + len(inputs))
			logID := int32(1 + len(inputs))
			planID := int32(1 + len(inputs)/2000) // 每 2000 个一个计划
			taskID := planID
			username := fmt.Sprintf("ssh_user_%d", accountID)

			pc := buildOnePassContext(ctx, "SSH", host, username,
				assetID, accountID, logID, taskID, planID)
			inputs = append(inputs, pc)
		}
	}

	// ---- RDP: 10,000 个 (200 资产 × 50 账号) ----
	for i := 0; i < 200; i++ {
		assetID := int32(20001 + i)
		host := fmt.Sprintf("10.1.0.%d", 101+i)
		for j := 0; j < 50; j++ {
			accountID := int32(2_000_001 + len(inputs))
			logID := int32(1 + len(inputs))
			planID := int32(36 + len(inputs)/1000) // 从 plan 36 开始
			taskID := planID
			username := fmt.Sprintf("rdp_user_%d", accountID)

			pc := buildOnePassContext(ctx, "RDP", host, username,
				assetID, accountID, logID, taskID, planID)
			inputs = append(inputs, pc)
		}
	}

	// ---- DB: 20,000 个 ----
	// MySQL: 8,000 (80 资产 × 100 账号)
	for i := 0; i < 80; i++ {
		assetID := int32(30001 + i)
		host := fmt.Sprintf("10.2.0.%d", 101+i)
		for j := 0; j < 100; j++ {
			accountID := int32(3_000_001 + len(inputs))
			logID := int32(1 + len(inputs))
			planID := int32(46 + len(inputs)/2000)
			taskID := planID
			username := fmt.Sprintf("mysql_user_%d", accountID)

			pc := buildOnePassContext(ctx, "MYSQL", host, username,
				assetID, accountID, logID, taskID, planID)
			inputs = append(inputs, pc)
		}
	}
	// Oracle: 6,000 (60 资产 × 100 账号)
	for i := 0; i < 60; i++ {
		assetID := int32(30101 + i)
		host := fmt.Sprintf("10.2.1.%d", 101+i)
		for j := 0; j < 100; j++ {
			accountID := int32(3_000_001 + len(inputs))
			logID := int32(1 + len(inputs))
			planID := int32(46 + len(inputs)/2000)
			taskID := planID
			username := fmt.Sprintf("oracle_user_%d", accountID)

			pc := buildOnePassContext(ctx, "ORACLE", host, username,
				assetID, accountID, logID, taskID, planID)
			inputs = append(inputs, pc)
		}
	}
	// PostgreSQL: 6,000 (60 资产 × 100 账号)
	for i := 0; i < 60; i++ {
		assetID := int32(30201 + i)
		host := fmt.Sprintf("10.2.2.%d", 101+i)
		for j := 0; j < 100; j++ {
			accountID := int32(3_000_001 + len(inputs))
			logID := int32(1 + len(inputs))
			planID := int32(46 + len(inputs)/2000)
			taskID := planID
			username := fmt.Sprintf("pg_user_%d", accountID)

			pc := buildOnePassContext(ctx, "POSTGRES", host, username,
				assetID, accountID, logID, taskID, planID)
			inputs = append(inputs, pc)
		}
	}

	return inputs
}
```

### 5.3 注入 Runner

```go
func TestInject100k(ctx context.Context) {
	runner := NewRunner(&RunnerConfig{
		Name:        "100kTest",
		BlockSize:   512,
		Concurrency: 128,
		ProcessFn:   mockProcessFn, // 根据 service sleep 随机时长后返回成功
		PreBatchFn:  mockBatchFn,
		PostBatchFn: mockBatchFn,
		ResourceLimits: map[Resource]int{
			"exec":              65,
			"sshExec":           128,
			"telnetExec":        128,
			"rdpScriptExec":     64,
			"dbSqlshellExec":    128,
			"dbPerAsset":        2,
			"sshPerAsset":       10,
			"rdpWinrmPerAsset":  5,
			"rdpScriptPerAsset": 3,
		},
	})
	runner.Start()

	inputs := build100kInputs(ctx)
	for _, pc := range inputs {
		runner.Process(ctx, pc)
	}
}
```

### 5.4 mockProcessFn 实现（按服务随机耗时）

```go
var serviceTimeRange = map[string][2]int{
	"SSH":      {5, 30},
	"RDP":      {60, 180}, // Script 隧道
	"MYSQL":    {10, 45},
	"ORACLE":   {15, 60},
	"POSTGRES": {10, 40},
}

func mockProcessFn(msgCtx MessageContext) {
	pc := msgCtx.(*PassContext)
	svc := pc.Request.MajorService
	rng := serviceTimeRange[svc]
	if rng[0] == 0 {
		rng = [2]int{5, 30} // fallback
	}
	sleepMs := rng[0]*1000 + rand.Intn((rng[1]-rng[0])*1000)
	time.Sleep(time.Duration(sleepMs) * time.Millisecond)
	// 默认成功，可注入失败逻辑
}
```

---

## 6. 验证清单

- [ ] 每个输入的 `Labels()` 按第 4 节产出正确的 Label 集合
- [ ] 同一资产的不同账号共享相同的 PerAsset Label Value
- [ ] 不同资产的 PerAsset Label Value 不同（hash 不同）
- [ ] SSH 70,000 输入不携带 `exec` Label
- [ ] RDP 10,000 输入携带 `exec` + `rdpScriptExec` + `rdpScriptPerAsset`
- [ ] DB 20,000 输入携带 `exec` + `dbSqlshellExec` + `dbPerAsset`
- [ ] Runner 运行时 `ResourceExec` 并发 ≤ 65
- [ ] Runner 运行时单 DB 资产并发 ≤ 2
- [ ] Runner 运行时单 RDP 资产并发 ≤ 3
- [ ] Runner 运行时单 SSH 资产并发 ≤ 10
