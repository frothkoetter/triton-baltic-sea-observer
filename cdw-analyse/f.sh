curl \
-X POST "https://ml-6ba30765-39d.se-sandb.a465-9q4k.cloudera.site/namespaces/serving-default/endpoints/se-sandbox-aws-caii-mistral/v1/chat/completions" \
-H "Content-Type: application/json" \
-H "Authorization: Bearer ${TOKEN}" \
-d '{
	"messages": [{ "role": "user", "content": "You are a helpful assistant." }],
	"model": "mistralai/mistral-7b-instruct-v0.3"
}'
