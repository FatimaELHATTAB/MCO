include:
  - project: 'Production-mutualisee/IPS/IDO/gitlab-cicd/pipelines'
    file: '.gitlab-ci.yml'

image: image-registry.openshift-image-registry.svc:5000/ns001b004551/cloudtools:1.4.0

stages:
  - version
  - run
  - build
  - install_pytest
  - test
  - code_quality

# --- 1) Info rapide d'env ---
version-job:
  stage: version
  script:
    - echo "BRANCH=$CI_COMMIT_BRANCH  SHA=$CI_COMMIT_SHORT_SHA  JOB=$CI_JOB_ID"
    - python3 --version || true

# --- 2) Build (reprend ta logique) ---
build-job-matching:
  stage: build
  rules:
    - if: '$CI_COMMIT_BRANCH == "master"'
  script:
    - |
      if [ "${CI_COMMIT_BRANCH}" ]; then
        export APP_IMAGE_TAG="${CI_COMMIT_BRANCH}-${APP_VERSION}-${CI_JOB_ID}"
      else
        export APP_IMAGE_TAG="${APP_VERSION}-${CI_JOB_ID}"
      fi
      export FULL_IMAGE_TAG="${DOCKER_PROJECT_ARTIFACTORY_REGISTRY}/matching:${APP_IMAGE_TAG}"
      echo "IMAGE=${FULL_IMAGE_TAG}"

    - echo "Login registries…"
    - echo "$ARTIFACTORY_PASSWORD" | docker login "$ARTIFACTORY_DOCKER_REGISTRY" -u "$ARTIFACTORY_USER" --password-stdin
    - echo "$ARTIFACTORY_PASSWORD" | docker login "$CI_REGISTRY/$PROJECT_ID-$ARTIFACTORY_DOCKER_REGISTRY" -u "$ARTIFACTORY_USER" --password-stdin || true

    - echo "Docker build…"
    - docker build --network host -f Matching/Dockerfile -t "$FULL_IMAGE_TAG" .
    - docker push "$FULL_IMAGE_TAG"

    - mkdir -p image-tag/"${APP_NAME}"
    - echo "${APP_IMAGE_TAG}" > image-tag/"${APP_NAME}"/value.txt
    - echo "${APP_IMAGE_TAG}" > image-tag/value.txt
  artifacts:
    paths:
      - image-tag/

# --- 3) Tests unitaires (repo Matching) ---
pytest-matching:
  stage: test
  image: image-registry.openshift-image-registry.svc:5000/ns001b004551/python:1.0.0
  variables:
    PYTHONPATH: "$CI_PROJECT_DIR"
    PIP_CACHE_DIR: "$CI_PROJECT_DIR/.cache/pip"
  cache:
    paths: [ .cache/pip ]
  before_script:
    - python3 --version
    - pip3 install --upgrade pip
    - pip3 install -r Matching/requirements.txt || true
    - pip3 install pytest pytest-cov
  script:
    - cd Matching
    - echo "Run pytest…"
    - pytest tests -q --maxfail=1 --disable-warnings \
        --junitxml=../junit_matching.xml \
        --cov=src \
        --cov-report=term-missing \
        --cov-report=xml:../coverage_matching.xml
  artifacts:
    when: always
    expire_in: 7 days
    paths:
      - junit_matching.xml
      - coverage_matching.xml
    reports:
      junit: junit_matching.xml
      coverage_report:
        coverage_format: cobertura
        path: coverage_matching.xml
  rules:
    - if: $CI_COMMIT_BRANCH

# --- 4) Sonar (lit les rapports du job de test) ---
running-sonar-code-quality:
  stage: code_quality
  image: image-registry.openshift-image-registry.svc:5000/ns001b004551/sonarscanner:4.7-n14
  needs: ["pytest-matching"]
  variables:
    SONAR_HOST_URL: "$SONAR_HOST_URL"   # à définir dans Settings > CI/CD > Variables
    SONAR_TOKEN: "$SONAR_TOKEN"         # token Sonar
  script:
    - echo "Run SonarScanner…"
    - sonar-scanner
      -Dproject.settings=sonar-matching.properties
      -Dsonar.host.url=$SONAR_HOST_URL
      -Dsonar.login=$SONAR_TOKEN
      -Dsonar.projectVersion=${APP_VERSION}
      -Dsonar.branch.name=${CI_COMMIT_BRANCH}
      -Dsonar.analysis.buildId=${CI_JOB_ID}
      -Dsonar.qualitygate.wait=true
  rules:
    - if: $CI_COMMIT_BRANCH



sonar.projectKey=p-4512-tpn_exit_promethee_python
sonar.projectName=tpn_exit_promethee_python_matching

sonar.sources=Matching/src
sonar.tests=Matching/tests
sonar.python.version=3.11
sonar.sourceEncoding=UTF-8

sonar.python.coverage.reportPaths=coverage_matching.xml
sonar.junit.reportPaths=junit_matching.xml
