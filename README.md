# flink_demo
some code about flink development

### design scheme
复杂嵌套json的scheme定义举例
```json
{
type: 'object',
properties: {
	name:{type:'string'},
	data:{
		type:'object',
		properties:{
		ccount:{
			type:'integer'
		},
		ctimestamp:{
			type:'date-time'
		}
		}}
	}
}
```