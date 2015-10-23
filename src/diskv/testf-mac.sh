go test 2> run.err | tee run.log | ggrep -P '^(--- )?FAIL|Passed|^Test:|^ok'
