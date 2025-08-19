import json
import os
import boto3
import openai
from typing import Dict, Any

# SQS 클라이언트 초기화
sqs = boto3.client('sqs')

def lambda_handler(event, context):
    """
    AWS Lambda function to process SQS messages and refine draft content using OpenAI
    """
    
    # Initialize OpenAI API key
    openai.api_key = os.environ['OPENAI_API_KEY']
    
    # 다음 큐 URL (환경 변수로 설정)
    next_queue_url = os.environ.get('NEXT_QUEUE_URL')
    
    try:
        # Process each record from SQS
        for record in event['Records']:
            # Parse the SQS message body
            message_body = json.loads(record['body'])
            
            # Extract draft generation event data
            task_id = message_body['taskId']
            request_prompt = message_body['requestPrompt']
            byte_count = message_body['byteCount']
            version = message_body['version']
            draft_content = message_body['draftContent']
            
            # Refine the draft content using OpenAI
            refined_content = refine_draft_content(
                draft_content, 
                request_prompt
            )
            
            # 개선된 내용을 다음 큐로 전송
            if next_queue_url:
                send_to_next_queue(
                    task_id=task_id,
                    request_prompt=request_prompt,
                    byte_count=byte_count,
                    version=version,
                    refined_content=refined_content,
                    queue_url=next_queue_url
                )
            
            print(f"Processed and forwarded task_id: {task_id}")
            
        return {
            'statusCode': 200,
            'body': json.dumps('Successfully processed all messages')
        }
        
    except Exception as e:
        print(f"Error processing messages: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }


def refine_draft_content(draft_content: str, original_prompt: str) -> str:
    """
    생활기록부 제약 조건을 확인하고 초안을 개선하는 함수
    """
    try:
        # 새로운 프롬프팅 전용 시스템 프롬프트
        system_prompt = """다음은 학생 생활기록부 초안이야. 아래 '작성 금지 규정'에 따라, 위반되는 내용을 모두 찾아내서 수정하거나 일반적인 용어로 대체해 줘. 원본의 의미는 최대한 유지해 줘.

## 작성 금지 규정

회사/브랜드명: '유튜브'는 '온라인 동영상 플랫폼', 'ChatGPT'는 '인공지능 챗봇'으로 변경

어학/자격증: 'TOEIC', 'HSK', '한자능력검정' 등 명칭 언급 금지

수상/대회: 'OO 경진대회에서 수상' 또는 'XX대회에 참여'와 같은 내용 모두 삭제

기관명: 허용된 교육기관 외의 특정 대학, 기관명 언급 금지

외국어: '프로젝트'는 '과제', '리더'는 '대표' 등 모든 영어 단어를 한글로 변경"""
        
        # 원본 바이트 수 계산
        original_byte_count = len(draft_content.encode('utf-8'))
        
        user_prompt = f"""## 검토할 초안 원문
{original_prompt}"""
        
        # OpenAI v0.28.1 API 사용
        combined_prompt = f"{system_prompt}\n\n{user_prompt}"
        
        response = openai.Completion.create(
            model="gpt-3.5-turbo-instruct",
            prompt=combined_prompt,
            temperature=0.5,
            max_tokens=2000
        )
        
        refined_content = response.choices[0].text.strip()
        
        # 프롬프트 설명 부분 제거 (이중 안전장치)
        lines = refined_content.split('\n')
        cleaned_lines = []
        
        for line in lines:
            # 설명성 텍스트 패턴 제거
            line_stripped = line.strip()
            if not (line_stripped.startswith('개선된') or 
                   line_stripped.startswith('초안') or
                   line_stripped.startswith('다음은') or
                   line_stripped.startswith('위의') or
                   line_stripped.startswith('수정된') or
                   line_stripped.startswith('다듬어진') or
                   ('바이트' in line_stripped and ':' in line_stripped) or
                   line_stripped.endswith(':') and len(line_stripped) < 30):
                cleaned_lines.append(line)
        
        refined_content = '\n'.join(cleaned_lines).strip()
        
        # 혹시 전체가 비어있으면 원본 반환
        if not refined_content:
            refined_content = draft_content
        
        # 바이트 수 비교
        refined_byte_count = len(refined_content.encode('utf-8'))
        byte_ratio = refined_byte_count / original_byte_count if original_byte_count > 0 else 0
        
        print(f"Byte count change: {original_byte_count} -> {refined_byte_count} (ratio: {byte_ratio:.2f})")
        
        # 분량이 너무 줄어들었으면 경고
        if byte_ratio < 0.7:
            print(f"Warning: Content significantly reduced (ratio: {byte_ratio:.2f})")
        
        return refined_content
        
    except Exception as e:
        print(f"Error calling OpenAI API: {str(e)}")
        raise e

def send_to_next_queue(task_id: str, request_prompt: str, 
                      byte_count: int, version: int, refined_content: str, queue_url: str) -> None:
    """
    개선된 내용을 다음 SQS 큐로 전송하는 함수
    """
    try:
        message_body = {
            'task_id': task_id,
            'request_prompt': request_prompt,
            'byte_count': byte_count,
            'version': version,
            'draft_content': refined_content,  # 개선된 내용으로 업데이트
        }
        
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message_body, ensure_ascii=False)
        )
        
        print(f"Message sent to queue: {queue_url}, MessageId: {response['MessageId']}")
        
    except Exception as e:
        print(f"Error sending message to SQS: {str(e)}")
        raise e