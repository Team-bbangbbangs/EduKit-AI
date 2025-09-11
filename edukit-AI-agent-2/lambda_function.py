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
            task_id = message_body['task_id']
            request_prompt = message_body['request_prompt']
            byte_count = message_body['byte_count']
            target_bytes = byte_count   
            min_bytes = byte_count - 100

            version = message_body['version']
            draft_content = message_body['draft_content']

            processed_message = {
                'task_id': task_id,
                'version': version,
                "status": "PHASE3_STARTED"
            }
            try:
                stream_id = redis_client.xadd('ai-response', {'data': json.dumps(processed_message, ensure_ascii=False)})
                print(f"Successfully added to Redis stream 'ai-response'. Stream ID: {stream_id}")
            except Exception as redis_error:
                print(f"Error adding to Redis stream: {str(redis_error)}")

            # Review the draft content using OpenAI
            final_content = review_student_record(
                draft_content=draft_content,
                target_bytes=target_bytes,
                min_bytes=min_bytes
            )
            
            print(f"Successfully reviewed draft. Byte count: {len(final_content.encode('utf-8'))}/{target_bytes}")
            
            # Publish reviewed content to Redis Stream
            message_data = {
                'task_id': task_id,
                'version': version,
                'final_content': final_content,
                "status": "COMPLETED"
            }
            
            try:
                stream_id = redis_client.xadd('ai-response', {'data': json.dumps(message_data, ensure_ascii=False)})
                print(f"Successfully added to Redis stream 'ai-response'. Stream ID: {stream_id}")
            except Exception as redis_error:
                print(f"Error adding to Redis stream: {str(redis_error)}")
            
        return {'statusCode': 200, 'body': json.dumps('Successfully processed all messages')}
        
    except Exception as e:
        print(f"Error processing messages: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps(f'Error: {str(e)}')}

def review_student_record(draft_content: str, target_bytes: int, min_bytes: int) -> str:
    """
    학생 생활기록부 초안을 스타일 가이드에 맞춰 최종 검토하는 함수
    """
    try:
        prompt = create_review_prompt(draft_content, target_bytes, min_bytes)
        
        # OpenAI 라이브러리를 사용한 API 호출
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "당신은 학생 생활기록부 작성 전문가입니다. 주어진 스타일 가이드를 정확히 따라 원고를 완성해 주세요."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=2000
        )
        
        reviewed_content = response.choices[0].message.content.strip()
        
        byte_count = len(reviewed_content.encode('utf-8'))
        print(f"Review completed. Original: {len(draft_content.encode('utf-8'))} bytes -> Reviewed: {byte_count} bytes")
        
        if byte_count < min_bytes:
            print(f"Warning: Content is below minimum bytes ({byte_count} < {min_bytes})")
        
        return reviewed_content
        
    # OpenAI 라이브러리의 공식 에러 처리 방식
    except APIError as e:
        print(f"OpenAI API error: {e}")
        return draft_content # API 호출 실패시 원본 반환
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        return draft_content

def create_review_prompt(draft_content: str, target_bytes: int, min_bytes: int) -> str:
    """학생 생활기록부 검토 프롬프트를 생성합니다."""
    prompt = f"""
    다음은 학생 생활기록부 원고야. 아래 '스타일 가이드'에 맞춰 최종본을 완성해 줘.

    ## 스타일 가이드

    목표 분량: 각 버전은 UTF-8 인코딩 기준 {target_bytes}Byte (최소 {min_bytes}byte 이상)로 맞춰 줘.
    문체: 모든 문장은 '~함.' 또는 '~임.'으로 끝나는 현재형 음슴체로 변경하고, 문장 끝에 온점을 붙여 줘.
    구두점: 쉼표(,)는 사용하지 말고, 의미가 명확하도록 문장을 다듬어 줘.
    표현: '학생은', '학생이' 같은 표현은 사용하지 마.

    ## 원고

    {draft_content}

    ## 최종본

    위의 스타일 가이드를 정확히 따라 최종본을 작성해 줘. 설명이나 부연설명 없이 완성된 본문만 출력해.
    """
    
    return prompt