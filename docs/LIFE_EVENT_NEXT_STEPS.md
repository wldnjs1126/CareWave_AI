# CareWave life_event 정리 (초기 단계)

이 브랜치는 **기존 실시간 센서 서비스와 DB를 변경하지 않고**, 새로운 이벤트 저장 계층을 준비합니다.

## 현재 확인한 운영 상태

- `life_event` 기존 종류: `PRESENCE`, `MOVEMENT`, `BED_STAY`, `SLEEP`, `ABSENCE`, `NIGHT_EXIT`, `INACTIVITY`
- 합계 439건 (2026-10-20 사용자 HeidiSQL 조회 결과)
- `FALL`은 학습/검증된 데이터 또는 이벤트가 아직 확인되지 않음
- 7개 **감지 범주**이지만 `PRESENCE`와 `ABSENCE`를 분리하므로 event_type은 총 8개
- `risk_level`은 임시 운영정책: 정상/관찰/주의/위험. AI 예측값이 아님.

## 제공 파일

| 파일 | 목적 |
|---|---|
| `event_engine/risk_policy.py` | 8개 event_type -> 4개 위험 등급 |
| `database/life_event_repository.py` | 이벤트 시작/종료 DB 함수 (아직 자동 호출하지 않음) |
| `scripts/check_life_event_ready.py` | 조회만 수행하는 DB 상태 검사 |
| `sql/01_add_life_event_risk_level.sql` | 수동, 1회용 컬럼 추가 안내 |
| `sql/02_backfill_life_event_risk_level.sql` | 선택적 과거 데이터 등급 지정 |
| `tests/test_risk_policy.py` | 순수 Python 테스트 |

**기존** `database/db_storage.py`는 이전 시뮬레이터용이므로 그대로 두었습니다.

## 실행 순서 (Windows / VS Code)

1. HeidiSQL에서 `SHOW COLUMNS FROM life_event;` 실행하여 `risk_level` 존재 여부 확인.
2. 없으면 `sql/01_add_life_event_risk_level.sql`의 ALTER 문을 **한 번만** 수동 실행.
3. 선택적으로 과거 이벤트에 적용할 정책을 확인하고 `sql/02_backfill_life_event_risk_level.sql` 사용.
4. 프로젝트 루트(`C:\CareWave_AI`)에서:

```powershell
conda activate carewave
python -m unittest discover -s tests -v
python -m scripts.check_life_event_ready
```

스키마 점검 도구는 DB를 **변경하지 않습니다**. `SQLAlchemy`, `PyMySQL`, `python-dotenv`가 없는 경우 해당 패키지를 설치합니다. 기존 `.env`의 DB_HOST/PORT/USER/PASSWORD/NAME 설정을 사용합니다. `.env`는 Git에 업로드하지 않습니다.

## 실시간 연결 전 필수 확인

- 저장 계층은 현재 **비활성**입니다. 이 브랜치만 적용해도 새 이벤트가 자동 생성되지는 않습니다.
- `sensor_raw.presence`는 현재 서버가 추론한 **재실 상태**입니다. 순간 타깃 감지와 혼동하지 마세요.
- 실험 데이터에서 **4초 누락 구간**이 반복 발견되었습니다. 누락을 '부재'나 '미움직임'으로 간주하면 안 됩니다.
- 위치/높이의 물리적 의미와 실제 방/침대/출입구 좌표를 검증해야 합니다.
- 기존 `life_event`에 쓰는 프로그램이 있는지 먼저 확인하여 **이중 저장을 막아야 합니다**.
- `start_event`는 열린 이벤트를 조회해 재사용하지만 DB 수준의 동시성 유일성까지 보장하지 않습니다. **단일 writer**로 검증한 뒤 운영해야 합니다.
- `detect_at`, `start_time`, `end_time`은 동일한 한국시간(KST), timezone 없는 `datetime`으로 전달합니다.
- `confidence`는 실제 판단 근거가 있을 때만 사용하며, 모델이 없으면 임의 확률을 넣지 않습니다.
- 낙상은 아직 안전하게 검증되지 않았으며 운영 의료 알림으로 사용하지 않습니다.

## 다음 개발 목표

1. `CareWave_sensor` 기존 이벤트 저장 프로세스와 중복 여부 및 4초 누락 원인 확인
2. `sensor_raw` 1초 집계 + 연속 3초 분석(낙상은 원본 고속 스트림 필요성 검증)
3. 7종 이벤트 detector에 데이터 품질/상태 전이/종료 로직 구현
4. 독립된 테스트 시나리오, 재시작 복구, 중복 방지 검증
5. 공개 낙상 자료와 실제 CareWave 특징을 정렬한 Random Forest 학습 및 검증 후 통합

## 2026-10-20: CSV 누락 구간 보호 및 과거 더미데이터 분리

- `rule_replay.py`: 관측 전후의 **확정된 동일 재실 상태**만 최대 5초 누락 구간에 걸쳐 하나의 에피소드로 보존합니다. 누락 시간은 10초 감지·움직임·수면·미움직임의 연속 시간에 계산하지 않습니다.
- `sensor_raw_validation_01.csv` (322행) 재분석: 4초 누락 14곳, 재실 후보 14건 → 1건, 움직임 후보 19건 그대로, 전체 후보 20건. 5초 연결은 **일시적인 보호 규칙**일 뿐 센서 데이터 누락의 근본 원인을 수정하지 않습니다.
- `database/life_event_repository.py`의 `start_event()`에는 `session_started_at` 필수가 추가되었습니다. 동일 세션의 중복 진행 이벤트는 차단하고, 이전 세션의 미종료 **더미데이터는 재사용하지 않습니다**. 그러나 DB의 동시 writer 방지 유일성 제약은 아직 없으므로 단일 writer에서만 사용해야 합니다.
- 모든 새 파일은 기본적으로 실행되거나 DB에 쓰이지 않습니다. `python -m event_engine.rule_replay <CSV 경로>`는 **JSON 파일만 출력**합니다.

실행:
```powershell
cd C:\CareWave_AI
git pull --ff-only
python -m unittest discover -s tests -v
python -m event_engine.rule_replay ai_model\experiments\sensor_raw_validation_01.csv
```

**주의**: 지금 샘플 CSV의 `zone`은 `ROOM`이어서 `BED_STAY`, `SLEEP`, `NIGHT_EXIT`의 실제 검증이 불가능합니다. 좌표계/침대 분류 구현이 선행되어야 합니다. `FALL`은 검증된 낙상 모델이 없어 비활성화된 상태입니다.

## 2026-10-20: DB 기반 읽기 전용 이벤트 미리보기

실제 NCP MySQL `sensor_raw`를 읽어 CSV와 동일한 후보 이벤트 판별을 수행합니다.

```cmd
cd C:\CareWave_AI
git pull --ff-only
python -m unittest discover -s tests -v
python -m event_engine.db_replay --subject-id 1 --sensor-id 1 --since "2026-10-20 13:00:30" --until "2026-10-20 13:06:40"
```

- `--since`와 `--until`은 KST의 DB DATETIME 기준; 데이터가 없는 구간에는 이벤트도 나오지 않습니다.
- 대상자, 센서, 시간 범위를 **모두** 입력해야 합니다.
- DB에서는 `sensor_raw`에 대해 **SELECT만** 합니다. `life_event`를 읽거나 변경하지 않습니다.
- 결과는 로컬 `carewave_db_event_preview.json`에 생성됩니다.
- 최대 5,000행을 기본으로 허용하며 초과할 경우 분석을 중단합니다. `--max-rows`로 조절 가능합니다.
- 실제 기존 더미 기록은 수정되지 않습니다.
- `sensor_raw.presence`는 이미 서버가 추론한 재실 상태입니다. 시뮬레이터와 실제 대상자 데이터를 혼동하지 않도록 입력 시간대와 ID를 확인해야 합니다.
- DB에 자동 쓰기는 **다음 단계**이며, 다른 이벤트 생성 프로세스 존재 여부를 확인하고 추가 테스트를 마친 뒤 별도로 활성화합니다.

## 2026-10-20: 실제 314행 동작 검증 후 MOVEMENT 종료 조건 개선

- 실험 시간 13:00:30~13:06:40에 NCP MySQL에서 얻은 314개 1초 기록을 같은 범위의 CSV로 재현.
- 기존: 재실 1건, 움직임 18건, 누락 구간 14곳.
- 임시 종료 히스테리시스: 움직임이 감지된 후 **5초 연속으로 유효한 관측에서 움직임 조건 미충족** 시에만 종료; 데이터 누락 시에는 관측을 상상하지 않고 이벤트를 닫음.
- 같은 314행 시험에서 재실 1건, 움직임 **8건** (걷기 6건 + 앉기 구간의 오탐 의심 2건), 누락 14곳.
- 여전히 움직임 결과는 **후보**입니다. 앉은 상태의 오탐이 있으므로 DB에 그대로 저장하지 않습니다. 미래 구간에서도 동일한 변화가 나타나는지 독립 평가가 필요합니다.
- 실제 테스트는 서 있기 → 걷기 → 앉기이며 **침대 체류 데이터가 아닙니다**. 따라서 zone이 전부 ROOM인 것은 이 실험에선 자연스러울 수 있습니다. BED 감지는 별도 침대 실험에서 검증합니다.
- 이벤트 저장 활성화에 앞서 기존 저장 프로세스와 충돌 여부, 더미데이터 구분, 미종료 이벤트 복구, 모델 오탐을 검증합니다.

## 이어서 진행: 22개 테스트 및 DB 저장 충돌 사전 점검

```cmd
cd C:\CareWave_AI
git pull --ff-only
python -m unittest discover -s tests -v
python -m event_engine.db_replay --subject-id 1 --sensor-id 1 --since "2026-10-20 13:00:30" --until "2026-10-20 13:06:40"
python -m scripts.check_life_event_write_conflicts --subject-id 1 --sensor-id 1 --since "2026-10-20 13:00:30" --until "2026-10-20 13:06:40"
```

- 기대 단위 테스트: 22개.
- 기존 314개 기록을 이용한 잠정 MOVEMENT 종료 완충 적용 결과: 18건에서 8건. 걷기 6건, 앉기 2건 오탐 의심. 실험 외 데이터에서 재검증 전 자동 저장 금지.
- `check_life_event_write_conflicts.py`는 현재 실험시간대 이벤트와 더 오래된 미종료 이벤트를 **별도로 SELECT**하며 결코 수정하지 않습니다.
- 이 점검으로 다른 프로세스가 동시에 기록 중인지 판별할 수 없습니다. 실제 writer 도입 전 운영자 확인이 필요합니다.
- `BED_STAY`, `SLEEP`, `NIGHT_EXIT`의 실제 침대 실험은 아직 진행하지 않았고 `FALL` 모델도 미검증입니다.

## 2026-10-20: 완료 이벤트의 선택적 DB 저장 준비

\`event_engine/db_backfill.py\`는 **과거 수집 구간**에서 확정된 완료 이벤트만 저장 대상으로 검토합니다.

일반 실행은 DB **SELECT만 수행하는 미리보기**입니다.
\`\`\`cmd
cd C:\CareWave_AI
git pull --ff-only
python -m unittest discover -s tests -v
python -m event_engine.db_backfill --subject-id 1 --sensor-id 1 --since "2026-10-20 13:00:30" --until "2026-10-20 13:06:40"
\`\`\`

안전 장치:
- \`end_time IS NULL\`인 이벤트는 저장 대상에서 제외(실험 종료가 이벤트 종료를 뜻하지 않음).
- \`sensor_data_gap\`, \`unknown_movement_data\` 때문에 종료된 후보는 제외.
- \`FALL\`은 학습·검증 완료 전 금지.
- 위험 등급은 \`risk_policy.py\`의 임시 운영 규칙만 사용.
- 실제 저장 시에도 \`evidence\`에는 \`PROVISIONAL_NOT_CLINICALLY_VALIDATED\`로 표시하고 confidence는 NULL.
- 저장 기능은 **기본 비활성**이고 단일 이벤트 종류, 예상 건수 일치, 명시 확인 문자열을 모두 입력해야 사용 가능.
- 동일 이벤트를 다시 넣지 않도록 정확한 식별값을 비교하고 불일치 시 전체 트랜잭션 중단. 하지만 다른 writer의 동시 쓰기를 DB 제약으로 막는 것은 **아직 구현되지 않음**.
- 오래된 더미 데이터, 미종료 3개 이벤트는 변경하지 않음.
- **이 실험의 MOVEMENT 후보에는 앉아 있을 때 오탐이 포함될 수 있음**. 사용자 및 개발팀 검토 전 \`--apply\` 사용하지 마세요.
- 실제 운영은 본 도구가 아닌 앞으로 개발할 실시간 상태 엔진으로 연결할 예정입니다.

현재 개발 단계: 데이터 검증 → 규칙 개선 → DB 저장 인터페이스 구성 완료(테스트 필요) → 검증된 자동 저장 활성화 → 낙상 모델 통합.
