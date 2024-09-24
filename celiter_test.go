package celiter_test

import (
	"fmt"
	"iter"
	"slices"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/decls"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/picatz/celiter"
	"github.com/shoenig/test/must"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name  string
		expr  string
		check func(t *testing.T, val ref.Val, err error)
	}{
		{
			name: "true exists expression",
			expr: "values().exists(x, x == 'test')",
			check: func(t *testing.T, val ref.Val, err error) {
				must.NoError(t, err)
				must.Eq(t, fmt.Sprintf("%v", val), "true")
			},
		},
		{
			name: "false exists expression",
			expr: "values().exists(x, x == 'notfound')",
			check: func(t *testing.T, val ref.Val, err error) {
				must.NoError(t, err)
				must.Eq(t, fmt.Sprintf("%v", val), "false")
			},
		},
		{
			name: "true index expression",
			expr: "values()[0] == 'test'",
			check: func(t *testing.T, val ref.Val, err error) {
				must.NoError(t, err)
				must.Eq(t, fmt.Sprintf("%v", val), "true")
			},
		},
		{
			name: "false index expression",
			expr: "values()[0] == 'notest'",
			check: func(t *testing.T, val ref.Val, err error) {
				must.NoError(t, err)
				must.Eq(t, fmt.Sprintf("%v", val), "false")
			},
		},
		{
			name: "invalid index expression",
			expr: "values()[4] == 'test'",
			check: func(t *testing.T, val ref.Val, err error) {
				must.Error(t, err)
			},
		},
		{
			name: "size expression",
			expr: "size(values()) == 3",
			check: func(t *testing.T, val ref.Val, err error) {
				must.NoError(t, err)
				must.Eq(t, fmt.Sprintf("%v", val), "true")
			},
		},
		{
			name: "true in expression",
			expr: "'test' in values()",
			check: func(t *testing.T, val ref.Val, err error) {
				must.NoError(t, err)
				must.Eq(t, fmt.Sprintf("%v", val), "true")
			},
		},
		{
			name: "false in expression",
			expr: "'blah' in values()",
			check: func(t *testing.T, val ref.Val, err error) {
				must.NoError(t, err)
				must.Eq(t, fmt.Sprintf("%v", val), "false")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				values      = []string{"test", "example", "sample"}
				valuesIndex = 0

				valuesIterable = celiter.New(
					func() (bool, error) {
						return valuesIndex < len(values), nil
					},
					func() (string, error) {
						val := values[valuesIndex]
						valuesIndex++
						return val, nil
					},
					func(s string) ref.Val {
						return types.String(s)
					},
				)
			)

			env, err := cel.NewEnv(
				cel.Function(
					"values",
					cel.Overload(
						"test_values",
						[]*cel.Type{},
						celiter.Type,
						decls.FunctionBinding(func(_ ...ref.Val) ref.Val {
							return valuesIterable
						}),
					),
				),
			)
			if err != nil {
				t.Fatalf("failed to create CEL environment: %v", err)
			}

			ast, issues := env.Compile(test.expr)
			if issues != nil {
				t.Fatalf("failed to compile CEL expression: %v", issues)
			}

			prg, err := env.Program(ast)
			if err != nil {
				t.Fatalf("failed to create CEL program: %v", err)
			}

			val, _, err := prg.Eval(map[string]any{})
			test.check(t, val, err)
		})
	}
}

func TestFromSeq(t *testing.T) {
	tests := []struct {
		name  string
		expr  string
		check func(t *testing.T, val ref.Val, err error)
	}{
		{
			name: "true exists expression",
			expr: "values().exists(x, x == 'test')",
			check: func(t *testing.T, val ref.Val, err error) {
				must.NoError(t, err)
				must.Eq(t, fmt.Sprintf("%v", val), "true")
			},
		},
		{
			name: "false exists expression",
			expr: "values().exists(x, x == 'notfound')",
			check: func(t *testing.T, val ref.Val, err error) {
				must.NoError(t, err)
				must.Eq(t, fmt.Sprintf("%v", val), "false")
			},
		},
		{
			name: "true index expression",
			expr: "values()[0] == 'test'",
			check: func(t *testing.T, val ref.Val, err error) {
				must.NoError(t, err)
				must.Eq(t, fmt.Sprintf("%v", val), "true")
			},
		},
		{
			name: "false index expression",
			expr: "values()[0] == 'notest'",
			check: func(t *testing.T, val ref.Val, err error) {
				must.NoError(t, err)
				must.Eq(t, fmt.Sprintf("%v", val), "false")
			},
		},
		{
			name: "invalid index expression",
			expr: "values()[4] == 'test'",
			check: func(t *testing.T, val ref.Val, err error) {
				must.Error(t, err)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env, err := cel.NewEnv(
				cel.Function(
					"values",
					cel.Overload(
						"test_values",
						[]*cel.Type{},
						celiter.Type,
						decls.FunctionBinding(func(_ ...ref.Val) ref.Val {
							return celiter.FromSeq(
								slices.Values([]string{"test", "example", "sample"}),
								func(v string) ref.Val {
									return types.String(v)
								},
							)
						}),
					),
				),
			)
			if err != nil {
				t.Fatalf("failed to create CEL environment: %v", err)
			}

			ast, issues := env.Compile(test.expr)
			if issues != nil {
				t.Fatalf("failed to compile CEL expression: %v", issues)
			}

			prg, err := env.Program(ast)
			if err != nil {
				t.Fatalf("failed to create CEL program: %v", err)
			}

			val, _, err := prg.Eval(map[string]any{})
			test.check(t, val, err)
		})
	}
}

func TestAsSeq(t *testing.T) {
	env, err := cel.NewEnv(
		cel.Function(
			"values",
			cel.Overload(
				"test_values",
				[]*cel.Type{},
				celiter.Type,
				decls.FunctionBinding(func(_ ...ref.Val) ref.Val {
					return celiter.FromSeq(
						slices.Values([]string{"test", "example", "sample"}),
						func(v string) ref.Val {
							return types.String(v)
						},
					)
				}),
			),
		),
	)
	if err != nil {
		t.Fatalf("failed to create CEL environment: %v", err)
	}

	ast, issues := env.Compile("values()")
	if issues != nil {
		t.Fatalf("failed to compile CEL expression: %v", issues)
	}

	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("failed to create CEL program: %v", err)
	}

	val, _, err := prg.Eval(map[string]any{})
	must.NoError(t, err)

	seq := celiter.AsSeq(val, func(v ref.Val) string {
		return v.Value().(string)
	})

	must.Eq(t, slices.Collect(seq), []string{"test", "example", "sample"})
}

func Test_Seq_Fibonacci(t *testing.T) {
	var fibSeq iter.Seq[int] = func(yield func(int) bool) {
		a, b := 0, 1
		for {
			if !yield(a) {
				return
			}
			a, b = b, a+b
		}
	}

	var fibSeqConvert = func(v int) ref.Val {
		return types.Int(v)
	}

	env, err := cel.NewEnv(
		cel.Function(
			"fibonacci",
			cel.Overload(
				"fibonacci_values",
				[]*cel.Type{},
				celiter.Type,
				decls.FunctionBinding(func(_ ...ref.Val) ref.Val {
					return celiter.FromSeq(
						fibSeq,
						fibSeqConvert,
					)
				}),
			),
		),
	)
	if err != nil {
		t.Fatalf("failed to create CEL environment: %v", err)
	}

	ast, issues := env.Compile("fibonacci()[10]") // 11th fibonacci number
	if issues != nil {
		t.Fatalf("failed to compile CEL expression: %v", issues)
	}

	prg, err := env.Program(ast)
	if err != nil {
		t.Fatalf("failed to create CEL program: %v", err)
	}

	val, _, err := prg.Eval(map[string]any{})
	must.NoError(t, err)

	must.Eq(t, val.Value().(int64), 55)
}
