go test 2> run.err | tee run.log | grep -P '^(--- )?FAIL|Passed|^Test:|^ok'
