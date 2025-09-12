import json
import os
import boto3
import redis
from openai import OpenAI, APIError # openai 라이브러리 import

# SQS 클라이언트 초기화
sqs = boto3.client('sqs')

# Redis 클라이언트 초기화
redis_client = redis.Redis(
    host=os.environ.get('REDIS_HOST', 'localhost'),
    port=int(os.environ.get('REDIS_PORT', 6379)),
    password=os.environ.get('REDIS_PASSWORD'),
    decode_responses=True
)

# OpenAI 클라이언트 초기화 (API 키 자동 로드)
# 핸들러 바깥에서 초기화하여 연결을 재사용합니다.
client = OpenAI(api_key=os.environ['OPENAI_API_KEY'])

def lambda_handler(event, context):
    """
    SQS 메시지를 처리하고 OpenAI를 사용해 생기부 초안을 검토하는 Lambda 함수
    """
    try:
        # Process each record from SQS
        for record in event['Records']:
            message_body = json.loads(record['body'])
            
            # Extract draft generation event data
            task_id = message_body['taskId']
            request_prompt = message_body['requestPrompt']
            byte_count = message_body['byteCount']
            target_bytes = byte_count   
            min_bytes = byte_count - 100

            version = message_body['version']
            draft_content = message_body['draftContent']

            processed_message = {
                'task_id': task_id,
                'version': version,
                "status": "PHASE2_STARTED"
            }
            try:
                stream_id = redis_client.xadd('ai-response', {'data': json.dumps(processed_message, ensure_ascii=False)})
                print(f"Successfully added to Redis stream 'ai-response'. Stream ID: {stream_id}")
            except Exception as redis_error:
                print(f"Error adding to Redis stream: {str(redis_error)}")

            # 규정 기반 프롬프트로 초안 개선 수행
            refined_content = refine_draft_content(
                draft_content=draft_content,
                original_prompt=request_prompt,
                min_bytes=min_bytes
            )

            next_queue_url = os.environ.get('NEXT_QUEUE_URL')
            print(f"Successfully reviewed draft. Byte count: {len(refined_content.encode('utf-8'))}/{target_bytes}")

            # 개선된 내용을 다음 SQS 큐로 전송
            if next_queue_url:
                send_to_next_queue(
                    task_id=task_id,
                    request_prompt=request_prompt,
                    byte_count=byte_count,
                    version=version,
                    refined_content=refined_content,
                    queue_url=next_queue_url
                )
            
        return {'statusCode': 200, 'body': json.dumps('Successfully processed all messages')}
        
    except Exception as e:
        print(f"Error processing messages: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps(f'Error: {str(e)}')}

def refine_draft_content(draft_content: str, original_prompt: str, min_bytes: int) -> str:
    """
    생활기록부 제약 조건을 확인하고 초안을 개선하는 함수 (규정 기반 프롬프트 복구)
    """
    try:
        system_prompt = f"""
            다음은 학생 생활기록부 초안이야. 아래 '작성 원칙'에 따라, 초안인 '{draft_content}'을 다듬어줘. 문장을 다듬되 UTF-8 인코딩 기준 최소 {min_bytes}byte 이상으로 최대한 작성해줘.
            ## 작성 원칙
            - 입력된 정보가 부정적이더라도, 직접적이거나 간접적으로라도 부정적인 표현을 작성하면 안됩니다. 대신 앞으로의 개선방향에 대해서 작성해주세요. '~할 계획임'과 같이 추측성 표현은 작성하지 마세요.
            - 입력된 정보를 바탕으로 생활기록부를 작성할 때, 아래 사항들은 절대 작성하지 마세요.
                - 회사명, 상호명, 브랜드명, 서비스명, 사이트명을 포함하지 마세요. 실제 명칭을 그대로 언급하거나, 실존하는 웹사이트나 앱 이름이 응답에 포함되면 안됩니다. 모든 브랜드명, 플랫폼명은 반드시 일반화하거나 가명으로 대체해야 하며, 예시가 필요한 경우에도 포괄적인 용어로 대체하여 사용하세요.
                - 공익어학 시험 자격증명은 작성 불가능
                    - 영어(TOEIC, TOEFL, TEPS), 중국어(HSK), 일본어(JPT, JLPT), 프랑스어(DELF, DALF),  독일어(ZD,  TESTDAF,  DSH,  DSD),  러시아어(TORFL),  스페인어(DELE),  상공회의소한자시험,  한자능력검정,  실용한자,  한자급수자격검정,  YBM  상무한검,  한자급수인증시험,  한자자격검정 등
                - 교외 및 교내 대회 수상 경력은 기재할 수 없음. 수상 경력을 포함해서 대회에 참여했다는 사실 자체도 기입 불가능
                - 구체적인 특정 대학명, 기관명은 입력할 수 없음 단, 교육관련기관(교육부 및 소속기관(대한민국학술원, 국사편찬위원회, 국립국제교육원, 국립특수교육원, 교원소청심사위원회, 중앙교육연수원), 시도교육청 및 직속기관, 교육지원청 및 소속기관에 한함)의 경우 기관명을 입력할 수 있음.
                - 영어는 반드시 한글 단어로 대체하여 작성하고 신조어는 사용하지 말아주세요.
                - 학생의 점수와 평가는 입력하지 마세요.
            - '{original_prompt}'의 내용에 존재하지 않는 정보는 절대 추가하지 마세요.
        """

        # 원본 바이트 수 계산
        original_byte_count = len(draft_content.encode('utf-8'))

        user_prompt = f"## 검토할 초안 원문\n{original_prompt}"

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.5,
            max_tokens=2000
        )

        refined_content = response.choices[0].message.content.strip()

        # 프롬프트 설명 부분 제거 (이중 안전장치)
        lines = refined_content.split('\n')
        cleaned_lines = []
        for line in lines:
            line_stripped = line.strip()
            if not (
                line_stripped.startswith('개선된') or
                line_stripped.startswith('초안') or
                line_stripped.startswith('다음은') or
                line_stripped.startswith('위의') or
                line_stripped.startswith('수정된') or
                line_stripped.startswith('다듬어진') or
                ('바이트' in line_stripped and ':' in line_stripped) or
                (line_stripped.endswith(':') and len(line_stripped) < 30)
            ):
                cleaned_lines.append(line)

        refined_content = '\n'.join(cleaned_lines).strip()

        # 혹시 전체가 비어있으면 원본 반환
        if not refined_content:
            refined_content = draft_content

        # 바이트 수 비교
        refined_byte_count = len(refined_content.encode('utf-8'))
        byte_ratio = refined_byte_count / original_byte_count if original_byte_count > 0 else 0
        print(f"Byte count change: {original_byte_count} -> {refined_byte_count} (ratio: {byte_ratio:.2f})")
        if byte_ratio < 0.7:
            print(f"Warning: Content significantly reduced (ratio: {byte_ratio:.2f})")

        return refined_content
    except APIError as e:
        print(f"OpenAI API error: {e}")
        return draft_content
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        return draft_content

def send_to_next_queue(task_id: str, request_prompt: str, 
                      byte_count: int, version: int, refined_content: str, queue_url: str) -> None:
    """
    개선된 내용을 다음 SQS 큐로 전송하는 함수 (원상 복구)
    """
    try:
        message_body = {
            'task_id': task_id,
            'request_prompt': request_prompt,
            'byte_count': byte_count,
            'version': version,
            'draft_content': refined_content,
        }

        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message_body, ensure_ascii=False)
        )
        print(f"Message sent to queue: {queue_url}, MessageId: {response['MessageId']}")
    except Exception as e:
        print(f"Error sending message to SQS: {str(e)}")
        raise e