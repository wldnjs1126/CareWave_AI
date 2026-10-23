CareWave: 3초 활동 분류 첫 실습 (2026-10-20)
============================================
목표
- 기존 sensor_raw_test_30.csv로 추출된 3초 특징값을 사용하여
  STANDING / WALKING / SITTING 3개 행동을 구분하는 실습 모델 실행
- 낙상(FALL)이나 7종 life_event 모델이 아닙니다.
- 모델을 실제 위험 알림에 연결하지 마세요.

레이블 기준 (사용자가 '거의 맞다'고 확인한 대략적 전환 시각)
- 약 11:01:12 정지 -> 걷기
- 약 11:02:37 걷기 -> 앉기
- 안전 구간만 채택: STANDING 10:59:30~11:00:57,
  WALKING 11:01:27~11:02:22, SITTING 11:02:52~11:03:45
- 3초 특징 구간 전체가 해당 안전 구간 안에 들어간 데이터만 레이블링
- STANDING 61개, WALKING 39개, SITTING 39개; 73개 제외
- 데이터는 모두 같은 한 번의 실험에서 나온 서로 겹치는 구간입니다.

파일 설명
- carewave_features_3s_reviewed.csv: 전체 212구간 + 임시 레이블 및 제외 이유
- carewave_training_3activity_provisional.csv: 임시 레이블 있는 139구간
- label_carewave_3s.py: 레이블 생성 코드 (재현 가능)
- prepare_carewave_3s.py: 원본 sensor_raw CSV에서 3초 특징 추출 코드
- train_carewave_activity_demo.py: RandomForest 시연용 학습 코드

실행 방법 (Windows / VS Code)
1. VS Code에서 기존 CareWave_AI 프로젝트를 엽니다.
2. ai_model/experiments/ 폴더를 새로 만든 뒤 ZIP 파일 내용을 풉니다.
3. VS Code 터미널을 엽니다.
4. 설치된 환경에서 다음을 실행합니다.

   cd ai_model\experiments
   python -m pip install pandas scikit-learn joblib
   python train_carewave_activity_demo.py carewave_training_3activity_provisional.csv

예상 결과
- STANDING: 61, WALKING: 39, SITTING: 39 라는 출력
- carewave_activity_rf_DEMO_ONLY.joblib 생성
- 성능 점수는 표시하지 않음 (하나의 실험을 학습/시험으로 나누면 과대평가 위험)

다음 단계
- 별도 세션으로 3개 동작을 각 2~3회 기록하고 정확한 시각을 적어
  학습과 평가를 '세션별로 완전히 분리'할 수 있도록 준비합니다.
- 이후 확실한 센서값으로 기본 7종 이벤트 엔진과 life_event DB를 연결하고,
  낙상 공개 데이터와 검증된 자체 데이터를 이용해 낙상 모델을 개발합니다.
