package celiter

import (
	"fmt"
	"iter"
	"reflect"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
)

// Ensure the Value type implements the ref.Val interface,
// the traits.Iterator interface, and the traits.Iterable interface.
var (
	_ ref.Val         = (*Value[any])(nil)
	_ traits.Iterator = (*Value[any])(nil)
	_ traits.Iterable = (*Value[any])(nil)
)

// Type is the type of the iterable value. Use this when defining custom
// CEL functions that handle (or return) iterable values.
//
// These values are iterable, indexable, and have a size.
var Type = types.DynType.WithTraits(
	traits.IterableType | traits.IteratorType | traits.IndexerType | traits.SizerType | traits.ContainerType,
)

// HasNext is a function that checks if there is a next element in the iterable.
type HasNext func() (bool, error)

// Next is a function that retrieves the next element in the iterable.
type Next[T any] func() (T, error)

// Convert is a function that converts an element of type T to a ref.Val type
// which can be used in CEL expressions.
type Convert[T any] func(T) ref.Val

// New created a new iterable Value instance for use in CEL expressions.
func New[T any](hasNext HasNext, next Next[T], convert Convert[T]) *Value[T] {
	if hasNext == nil {
		hasNext = func() (bool, error) {
			return false, nil
		}
	}

	if next == nil {
		next = func() (T, error) {
			var zero T
			return zero, fmt.Errorf("no next element")
		}
	}

	if convert == nil {
		convert = func(t T) ref.Val {
			return types.DefaultTypeAdapter.NativeToValue(t)
		}
	}

	return &Value[T]{
		hasNext: hasNext,
		next:    next,
		convert: convert,
		index:   -1,
	}
}

// Value represents an iterable value in CEL expressions.
type Value[T any] struct {
	index   int
	cur     T
	hasNext HasNext
	next    Next[T]
	convert Convert[T]
}

// ConvertToNative converts the current iterable value to a native Go type.
func (v *Value[T]) ConvertToNative(typ reflect.Type) (any, error) {
	nativeValue := v.cur
	if reflect.TypeOf(nativeValue).AssignableTo(typ) {
		return nativeValue, nil
	}
	return nil, fmt.Errorf("unable to convert %s to native type %s", v.Type().TypeName(), typ.Name())
}

// ConvertToType converts the current iterable value to a ref.Val type.
func (ci *Value[T]) ConvertToType(typ ref.Type) ref.Val {
	return types.NewErr(fmt.Sprintf("unable to convert %s to type %s", ci.Type().TypeName(), typ.TypeName()))
}

// Equal checks if the current iterable value is equal to another ref.Val type.
func (ci *Value[T]) Equal(other ref.Val) ref.Val {
	if otherValue, ok := other.(*Value[T]); ok {
		return types.Bool(ci == otherValue)
	}

	return types.False
}

// Type returns the type of the iterable value.
func (ci *Value[T]) Type() ref.Type {
	return Type
}

// Value returns the current value of the iterable.
func (ci *Value[T]) Value() any {
	return ci.cur
}

// Next retrieves the next element in the iterable value.
func (ci *Value[T]) Next() ref.Val {
	next, err := ci.next()
	if err != nil {
		return types.NewErr("error getting next element: %w", err)
	}

	ci.cur = next

	ci.index++

	return ci.convert(next)
}

// HasNext checks if there is a next element in the iterable value.
func (ci *Value[T]) HasNext() ref.Val {
	hasNext, err := ci.hasNext()
	if err != nil {
		return types.NewErr("error checking for next element: %w", err)
	}

	return types.Bool(hasNext)
}

// Iterator returns the current iterable value, satisfying the traits.Iterator interface.
func (ci *Value[T]) Iterator() traits.Iterator {
	return ci
}

// Get retrieves the value at the given key index, allowing for random access of the
// iterable value using an index value (like an array).
func (v *Value[T]) Get(key ref.Val) ref.Val {
	if key.Type() != types.IntType {
		return types.NewErr("invalid key type for iterable: %s, must be int", key.Type())
	}

	keyValue := key.Value()
	keyIndex := int(keyValue.(int64))
	if keyIndex < 0 {
		return types.NewErr("index cannot be negative")
	}

	if keyIndex < v.index {
		return types.NewErr("index already passed")
	}

	for v.index < keyIndex {
		if !v.HasNext().Value().(bool) {
			return types.NewErr("index out of bounds during iterable access")
		}
		v.Next()
	}

	return v.convert(v.cur)
}

// Size returns the size of the iterable value.
func (v *Value[T]) Size() ref.Val {
	size := 0
	for v.HasNext().Value().(bool) {
		v.Next()
		size++
	}

	return types.Int(size)
}

// Contains checks if the iterable value contains the given value.
func (v *Value[T]) Contains(val ref.Val) ref.Val {
	for v.HasNext().Value().(bool) {
		if v.Next().Equal(val) == types.True {
			return types.True
		}
	}

	return types.False
}

// FromSeq creates a new iterable Value instance from a sequence of elements,
// which allows for simple interoperability between Go and CEL iterable types.
func FromSeq[T any](seq iter.Seq[T], convert Convert[T]) *Value[T] {
	var cur T

	next, stop := iter.Pull(seq)

	hasNext := func() (bool, error) {
		var ok bool
		cur, ok = next()
		if !ok {
			stop()
		}
		return ok, nil
	}

	getNext := func() (T, error) {
		return cur, nil
	}

	value := New(hasNext, getNext, convert)

	return value
}

// AsSeq converts a CEL iterable Value instance to a sequence of elements.
//
// # Important
//
//  1. If the iterable is not a CEL iterable, an empty sequence is returned.
//  2. If there are any errors during conversion, the sequence will be truncated,
//     or the program could panic.
//  3. Probably not a good idea to use this function in production code,
//     but really useful for testing, debugging, and REPL-like environments
//     where you want to quickly convert between CEL and Go types.
func AsSeq[T any](val ref.Val, convert func(ref.Val) T) iter.Seq[T] {
	if convert == nil {
		convert = func(val ref.Val) T {
			return val.Value().(T)
		}
	}

	iterVal, ok := val.(traits.Iterator)
	if !ok {
		return func(yield func(T) bool) {}
	}

	hasNext := func() (bool, error) {
		rv := iterVal.HasNext()
		return fmt.Sprintf("%v", rv) == "true", nil
	}

	getNext := func() (T, error) {
		return convert(iterVal.Next()), nil
	}

	return func(yield func(T) bool) {
		for {
			ok, err := hasNext()
			if err != nil || !ok {
				break
			}

			t, err := getNext()
			if err != nil {
				break
			}

			if !yield(t) {
				break
			}
		}
	}
}
