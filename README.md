## Lambda에 올릴 zip 파일 생성하는 법
### 1. 관련된 의존성 설치
```
pip install \
--platform manylinux2014_x86_64 \
--target=./package \
--implementation cp \
--python-version 3.13 \
--only-binary=:all: \
--upgrade \
-r requirements.txt
```
### 2. 의존성 압축
```
cd package/  

zip -r ../deployment-package.zip .    
```

### 3. 람다 함수 zip 파일에 추가
```
cd ..

zip -g deployment-package.zip lambda_function.py
```
