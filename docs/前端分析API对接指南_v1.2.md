# 鍓嶇鍒嗘瀽 API 瀵规帴鎸囧崡锛坴1.2锛?
闈㈠悜鍓嶇/AI 寮€鍙戝伐绋嬪笀鐨勫鎺ユ枃妗ｏ紝瑕嗙洊瀛︾姹囪仛銆佺淮搴︺€侀棶鍗烽鐩€夐」鍒嗗竷绛夊父鐢ㄦ暟鎹媺鍙栨柟寮忎笌娓叉煋瑕佺偣銆傛寜鏈枃鍗冲彲鐩存帴缂栧啓椤甸潰涓庡浘琛ㄦ覆鏌撻€昏緫銆?
---

## 1. 鍩烘湰淇℃伅

- 閮ㄧ讲涓庡熀鍧€锛堥粯璁ょ鍙ｏ級
  - 涓绘湇鍔★紙FastAPI锛夛細`http://<host>:8000`
  - Subjects v1.2 涓撶敤鏈嶅姟锛團astAPI锛屽彲涓庝富鏈嶅姟鍏卞瓨锛夛細`http://<host>:8001`
  - 鑻ュ彧鍚敤涓绘湇鍔★紝涔熷彲鍦ㄤ富鏈嶅姟鍚屽潃浣跨敤 `/api/v12/...` 绔偣銆?- 閴存潈锛氬綋鍓嶆棤閴存潈锛涘凡寮€鍚?CORS锛堝厑璁镐换鎰忔潵婧愶級銆?- 鐗堟湰鏍囪瘑锛氭湰鎸囧崡瀵瑰簲 v1.2 瑙勮寖銆傚搷搴斾綋 `data.schema_version` 涓?`data.data_version` 涓?`v1.2`銆?- 涓昏姒傚康
  - 鎵规 `batch_code`锛氬 `G4-2025`
  - 瀛︽牎 `school_id`/`school_code`锛氬 `5044`
  - 瀛︾ `subject_name`锛氬 `鏁板`銆乣闂嵎`
  - 瀛︾绫诲瀷 `type`锛歚exam`锛堣€冭瘯绫伙級銆乣questionnaire`锛堥棶鍗风被锛?
---

## 2. 缁熶竴 Subjects锛坴1.2锛?
缁熶竴杩斿洖鍖哄煙绾ф垨瀛︽牎绾х殑鈥滃绉戝垪琛?+ 鎸囨爣 + 缁村害鈥濓紝渚夸簬鍓嶇涓€娆℃€ф覆鏌撳绉戝崱鐗囥€佺淮搴﹂浄杈俱€佹帓鍚嶇瓑鏍稿績瑙嗗浘銆?
### 2.1 绔偣涓€瑙堬紙v1.2锛?
- 鑾峰彇鍖哄煙绾?subjects锛堟帹鑽愶級
  - `GET /api/v12/batch/{batch_code}/regional`
  - 杩斿洖锛歚{ success, message, code, data }`锛宍data.subjects` 涓哄绉戞暟缁勩€?- 鑾峰彇瀛︽牎绾?subjects锛堟帹鑽愶級
  - `GET /api/v12/batch/{batch_code}/school/{school_id}`
  - 杩斿洖锛氬悓涓娿€?- 瑙﹀彂鐗╁寲锛堢敓鎴愬苟鍐欏叆 DB锛岄€氬父鐢ㄤ簬棰勭儹/鍥炲～锛?  - `POST /api/v12/batch/{batch_code}/materialize`
  - 鍚庣浼氶亶鍘嗕富鏁版嵁瀛︽牎鐢熸垚骞跺啓鍏ワ紝鍓嶇鑱旇皟鏃堕€氬父鏃犻渶璋冪敤銆?
> 璇存槑锛氫互涓婄鐐瑰湪涓绘湇鍔′笌 subjects 涓撶敤鏈嶅姟鍧囧彲鐢紙`/api/v12/...`锛夈€?
### 2.2 鍝嶅簲缁撴瀯锛堝尯鍩?瀛︽牎閫氱敤锛?
```json
{
  "success": true,
  "message": "v1.2 鍖哄煙绾?subjects 宸茬敓鎴?G4-2025",
  "code": 200,
  "data": {
    "schema_version": "v1.2",
    "data_version": "v1.2",
    "batch_code": "G4-2025",
    "aggregation_level": "REGIONAL", // 鎴?"SCHOOL"
    "subjects": [
      {
        "subject_name": "鏁板",
        "type": "exam",
        "metrics": {
          "avg": 78.53,
          "stddev": 12.36,
          "min": 12.0,
          "max": 100.0,
          "difficulty": 0.79,            // 闅惧害绯绘暟锛堝緱鍒嗙巼锛?          "discrimination": 0.52,        // 鍖哄垎搴︼紙澧炲己缁熻璁＄畻锛?          "percentiles": {               // 鍖哄煙绾э細鍦?metrics.percentiles 鍐?            "P10": 56.0,
            "P50": 80.0,
            "P90": 95.0
          },
          "subject_full_score": 100.0,
          // 瀛︽牎绾э細浼氶檮甯?rank锛堝尯鍩熷悕娆★紝鏁板€艰秺灏忚秺闈犲墠锛?          "rank": 8
        },
        // 鍖哄煙绾э細瀛︽牎鎺掑悕锛圱OP N 鍙洿鎺ユ覆鏌撴鍗曪級
        "school_rankings": [
          { "school_id": "5044", "school_name": "涓€瀹?灏忓)", "avg": 85.12, "rank": 1 },
          { "school_id": "5068", "school_name": "浜屼腑(灏忓)",   "avg": 83.05, "rank": 2 }
        ],
        // 缁村害锛堣€冭瘯/闂嵎鍧囨敮鎸侊級銆?        // - 鑰冭瘯锛氱淮搴﹀惈鍧囧垎/寰楀垎鐜?锛堝鏍＄骇鍚?rank锛?        // - 闂嵎锛氱淮搴﹀惈 option_distribution锛堥€夐」鍗犳瘮鍒楄〃锛?        "dimensions": [
          {
            "code": "D-CUR",
            "name": "濂藉蹇?,
            "avg": 82.35,
            "score_rate": 0.82,
            "rank": 5, // 瀛︽牎绾ф湁鏁堬紱鍖哄煙绾ф棤 rank
            // 闂嵎缁村害涓嬬殑閫夐」鍒嗗竷锛堝尯鍩?瀛︽牎鍧囧彲鑳藉瓨鍦級
            "option_distribution": [
              { "option_level": 1, "option_label": "闈炲父涓嶇鍚?, "pct": 5.21 },
              { "option_level": 2, "option_label": "涓嶇鍚?,     "pct": 12.45 },
              { "option_level": 3, "option_label": "涓€鑸?,       "pct": 30.29 },
              { "option_level": 4, "option_label": "绗﹀悎",       "pct": 36.10 },
              { "option_level": 5, "option_label": "闈炲父绗﹀悎",   "pct": 15.95 }
            ]
          }
        ]
      }
    ]
  }
}
```

### 2.3 鍓嶇甯哥敤娓叉煋鏄犲皠

- 瀛︾鍗＄墖锛堝尯鍩?瀛︽牎锛?  - 鏍囬锛歚subject_name`
  - 绫诲瀷寰芥爣锛歚type === 'questionnaire' ? '闂嵎' : '鑰冭瘯'`
  - 鍏抽敭鎸囨爣锛歚metrics.avg`銆乣metrics.difficulty`銆乣metrics.stddev`銆乣metrics.min`/`max`
  - 鐧惧垎浣嶏紙鍖哄煙绾э級锛歚metrics.percentiles.P10/P50/P90`
  - 瀛︽牎鍚嶆锛堝鏍＄骇锛夛細`metrics.rank`
- 瀛︽牎鎺掑悕姒滐紙鍖哄煙绾э級锛歚school_rankings` 鎸?`rank` 鍗囧簭娓叉煋
- 缁村害闆疯揪/鏉″舰鍥撅紙鑰冭瘯/闂嵎锛?  - 鍚嶇О锛歚dimensions[].name`
  - 鏁板€硷細`dimensions[].avg` 鎴?`dimensions[].score_rate * 100`
  - 瀛︽牎绾х淮搴﹀悕娆★細`dimensions[].rank`
- 闂嵎缁村害閫夐」鍒嗗竷锛堝爢鍙犳潯褰?鐜浘锛?  - 鏁版嵁婧愶細`dimensions[].option_distribution`
  - 鏄剧ず锛歚option_label`锛堣嫢涓虹┖鍙洖閫€涓?`閫夐」{option_level}`锛夛紝鍗犳瘮鐢?`pct`

> 鎻愮ず锛氶棶鍗封€滈鐩骇鈥濈殑閫夐」鍒嗗竷涓嶅湪 subjects 鍐呭祵锛岄渶浣跨敤绗?3 鑺傜殑闂嵎鍒嗗竷 API銆?
---

## 3. 闂嵎棰樼洰閫夐」鍒嗗竷 API

棰樼洰绾ч€夐」鍒嗗竷鏉ヨ嚜鐙珛琛?`questionnaire_option_distribution`锛屽凡鍦ㄦ竻娲楅樁娈电墿鍖栵紝鍓嶇鍙洿鎺ヨ皟鐢ㄤ笅鍒?API 娓叉煋棰樼洰鍒嗗竷鍥捐〃銆?
### 3.1 绔偣涓€瑙?
- 鍒楀嚭鏈夐棶鍗锋暟鎹殑瀛︽牎锛堟寜绉戠洰锛?  - `GET /api/v1/questionnaire-distributions/{batch_code}/{subject_name}/schools`
  - 杩斿洖锛歚[{ school_id, school_name, student_count, has_distribution_data }]`

- 鍖哄煙绾ч鐩€夐」鍒嗗竷
  - `GET /api/v1/questionnaire-distributions/{batch_code}/{subject_name}/regional`
  - 杩斿洖缁撴瀯锛?    ```json
    {
      "batch_code": "G4-2025",
      "subject_name": "闂嵎",
      "level": "regional",
      "questions": [
        {
          "question_id": "Q1",
          "total_responses": 1532,
          "options": [
            { "option_level": 1, "option_label": "闈炲父涓嶇鍚?, "count": 120, "pct": 7.83 },
            { "option_level": 2, "option_label": "涓嶇鍚?,     "count": 210, "pct": 13.71 },
            { "option_level": 3, "option_label": "涓€鑸?,       "count": 520, "pct": 33.97 },
            { "option_level": 4, "option_label": "绗﹀悎",       "count": 470, "pct": 30.67 },
            { "option_level": 5, "option_label": "闈炲父绗﹀悎",   "count": 212, "pct": 13.83 }
          ]
        }
      ]
    }
    ```

- 瀛︽牎绾ч鐩€夐」鍒嗗竷
  - `GET /api/v1/questionnaire-distributions/{batch_code}/{subject_name}/school/{school_id}`
  - 杩斿洖缁撴瀯锛氫笌鍖哄煙绾х浉鍚岋紙鏃?`level` 瀛楁锛夛紝鎸夊鏍¤繃婊ゃ€?
> 鏍囩鏉ユ簮锛氫紭鍏?`questionnaire_scale_options` 鏍囧噯瀛楀吀锛涚己澶辨椂浣跨敤閫氱敤鏍囩鎴栧洖閫€ `閫夐」{level}`銆?
### 3.2 娓叉煋鎸囧紩

- 鏌辩姸/鍫嗗彔鏉″舰锛氭í杞?`option_label`锛岀旱杞?`pct`锛涘棰樼洰鏃剁敤 `question_id` 鍒嗙粍銆?- 鐜浘锛氫娇鐢ㄥ崟棰樼殑 `options[].pct`锛沗legend` 鏄剧ず `option_label`銆?- 瀵规瘮锛堝尯鍩?vs 瀛︽牎锛夛細鍒嗗埆鎷夊彇涓や釜绔偣锛屾寜 `question_id + option_level` 瀵归綈鍚庣粯鍒跺弻鏌便€?
---

## 4. 甯歌缁勫悎鏌ヨ涓庡墠绔皟鐢ㄧず渚?
浠ヤ笅绀轰緥浠ユ祻瑙堝櫒/Node 绔?`fetch`/`axios` 涓轰緥锛屽睍绀哄浣曡幏鍙栧苟缁勭粐鏁版嵁娓叉煋銆?
### 4.1 鑾峰彇鍖哄煙绾?subjects 骞舵覆鏌撳绉戝崱鐗?
```ts
// TS/JS 绀轰緥锛堝熀鍧€鎸夊疄闄呴儴缃叉浛鎹級
const BASE = "http://localhost:8000";

async function loadRegionalSubjects(batchCode: string) {
  const res = await fetch(`${BASE}/api/v12/batch/${batchCode}/regional`);
  const body = await res.json();
  if (!body?.success) throw new Error(body?.message || "璇锋眰澶辫触");
  const subjects = body.data?.subjects || [];
  return subjects.map((s: any) => ({
    name: s.subject_name,
    type: s.type,
    avg: s.metrics?.avg,
    stddev: s.metrics?.stddev,
    difficulty: s.metrics?.difficulty,
    p10: s.metrics?.percentiles?.P10,
    p50: s.metrics?.percentiles?.P50,
    p90: s.metrics?.percentiles?.P90,
    rankings: s.school_rankings || [],
    dimensions: s.dimensions || []
  }));
}
```

### 4.2 鑾峰彇瀛︽牎绾?subjects 骞舵覆鏌撶淮搴﹂浄杈?鍚嶆

```ts
async function loadSchoolSubjects(batchCode: string, schoolId: string) {
  const res = await fetch(`${BASE}/api/v12/batch/${batchCode}/school/${schoolId}`);
  const body = await res.json();
  if (!body?.success) throw new Error(body?.message || "璇锋眰澶辫触");
  const subjects = body.data?.subjects || [];
  // 浠ョ涓€涓绉戜负渚嬫瀯閫犻浄杈?  const first = subjects[0];
  const dims = (first?.dimensions || []).map((d: any) => ({
    name: d.name || d.code,
    value: d.avg ?? (d.score_rate ? d.score_rate * 100 : null),
    rank: d.rank
  }));
  const rank = first?.metrics?.rank; // 鍏ㄧ鐩湪鍖哄煙鍐呯殑鎺掑悕
  return { subjects, dims, rank };
}
```

### 4.3 娓叉煋闂嵎棰樼洰閫夐」鍒嗗竷锛堝尯鍩?瀛︽牎锛?
```ts
async function loadQuestionOptionDistribution(batch: string, subject: string, schoolId?: string) {
  const path = schoolId
    ? `/api/v1/questionnaire-distributions/${batch}/${subject}/school/${schoolId}`
    : `/api/v1/questionnaire-distributions/${batch}/${subject}/regional`;
  const res = await fetch(`${BASE}${path}`);
  const body = await res.json();
  if (body?.code !== 200) throw new Error(body?.message || "璇锋眰澶辫触");
  const qs = body?.data?.questions || [];
  // 灏嗗崟棰樿浆涓虹粯鍥惧弸濂界粨鏋?  return qs.map((q: any) => ({
    id: q.question_id,
    total: q.total_responses,
    options: (q.options || []).map((o: any) => ({ label: o.option_label || `閫夐」${o.option_level}`, pct: o.pct }))
  }));
}
```

### 4.4 瀛︽牎绛涢€夛紙闂嵎绉戠洰锛?
```ts
async function listSchoolsWithQuestionnaire(batch: string, subject: string) {
  const res = await fetch(`${BASE}/api/v1/questionnaire-distributions/${batch}/${subject}/schools`);
  const body = await res.json();
  if (body?.code !== 200) throw new Error(body?.message || "璇锋眰澶辫触");
  return body?.data?.schools || [];
}
```

---

## 5. 娓叉煋寤鸿涓庤竟鐣屽鐞?
- 鏁板€肩簿搴︼細鎵€鏈夌櫨鍒嗘瘮/鍧囧€煎凡缁熶竴鑷?2 浣嶅皬鏁帮紱鍓嶇鐩存帴鏄剧ず鎴栧啀鏍煎紡鍖栦负 `%` 绗﹀彿銆?- 闂嵎鏍囩缂哄け锛歚option_label` 鍙兘涓虹┖锛屽墠绔簲鍥為€€涓?`閫夐」{option_level}`銆?- 缁村害缂哄け锛氳嫢 `dimensions` 涓虹┖鎴栫淮搴﹀悕鏄犲皠缂哄け锛堝彧杩斿洖 `code`锛夛紝鍓嶇鍙檷绾ф樉绀?`code`銆?- 鎺掑悕鐩哥瓑锛氬尯鍩熷鏍℃帓鍚嶄娇鐢?`DENSE_RANK`锛涘潎鍒嗙浉鍚屽悕娆＄浉鍚岋紝鍚庣画鍚嶆鎸夊瘑闆嗘帓鍚嶉€掑銆?- 鎬ц兘锛氶〉闈㈤娆¤繘鍏ュ缓璁鍙栧尯鍩熺骇 subjects锛涘鏍″垏鎹㈡椂鍐嶆媺瀛︽牎绾?subjects/闂嵎鍒嗗竷銆?
---

## 6. 甯歌闂锛團AQ锛?
1) 涓轰綍闂嵎棰樼洰閫夐」鍒嗗竷涓嶅湪 subjects 閲岋紵
- v1.2 瑙勮寖灏嗛鐩垎甯冪嫭绔嬩负 `questionnaire_option_distribution` 琛ㄤ笌涓撶敤 API锛岄伩鍏嶅祵鍏ラ€犳垚浣撶Н鑶ㄨ儉涓庨噸澶嶆煡璇€?
2) 闂嵎缁村害閫夐」鍒嗗竷濡備綍娓叉煋锛?- 缁村害鍐呯殑 `option_distribution` 鐩存帴鐢ㄤ簬缁村害灞傚浘琛紱棰樼洰灞傚垎甯冮渶璋冪敤绗?3 鑺傜鐐广€?
3) 鍖哄煙绾?瀛︽牎绾х殑鈥滄暟鎹€婚噺鈥濆湪鍝噷锛?- 缁熻琛屾暟锛堝鎬诲鏍℃暟/鎬诲鐢熸暟锛夊啓鍏ヤ簡缁熻琛ㄧ殑缁撴瀯鍖栧垪锛宍subjects` JSON 閲屼笉蹇呭寘鍚紱鑻ョ‘闇€锛岃鍦ㄥ悗绔墿灞曡繑鍥炪€?
4) 闇€瑕佸厛鈥滅墿鍖栤€濆悧锛?- 璇诲彇绔細鍗虫椂鏋勫缓骞剁紦瀛樺啓鍏ワ紱鍓嶇閫氬父鐩存帴 `GET` 鍗冲彲锛屾棤闇€涓诲姩 `POST /materialize`銆?
---

## 7. 蹇€熸牎楠岋紙curl锛?
```bash
# 鍖哄煙绾?subjects
curl "http://localhost:8000/api/v12/batch/G4-2025/regional"

# 瀛︽牎绾?subjects
curl "http://localhost:8000/api/v12/batch/G4-2025/school/5044"

# 鍖哄煙绾ч棶鍗烽鐩垎甯?curl "http://localhost:8000/api/v1/questionnaire-distributions/G4-2025/闂嵎/regional"

# 瀛︽牎绾ч棶鍗烽鐩垎甯?curl "http://localhost:8000/api/v1/questionnaire-distributions/G4-2025/闂嵎/school/5044"

# 鎸夐棶鍗风鐩垪瀛︽牎锛堟槸鍚﹀凡鏈夊垎甯冿級
curl "http://localhost:8000/api/v1/questionnaire-distributions/G4-2025/闂嵎/schools"
```

---

## 8. 鍙樻洿涓庣増鏈?
- 2025-09-12锛氶娆″彂甯冿紙v1.2锛夈€?  - 缁熶竴 subjects锛堝尯鍩?瀛︽牎锛?  - 闂嵎棰樼洰閫夐」鍒嗗竷鐙珛 API
  - 缁村害/鎺掑悕/鐧惧垎浣嶇瓑鎸囨爣缁撴瀯璇存槑


