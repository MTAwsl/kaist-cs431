use std::sync::atomic::{AtomicI32, Ordering};
use std::thread::scope;

use cs431_homework::elim_stack::{ElimStack, Stack};

#[test]
fn push_example_simple() {
    let stack = ElimStack::default();

    scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..10 {
            let handle = scope.spawn(|| {
                for i in 0..10_000 {
                    stack.push(i);
                    assert!(stack.pop().is_some());
                }
            });
            handles.push(handle);
        }
    });

    assert!(stack.pop().is_none());
}

#[test]
fn push_pop_single_thread() {
    let stack = ElimStack::default();

    stack.push(1);
    stack.push(2);
    stack.push(3);

    assert_eq!(stack.pop(), Some(3));
    assert_eq!(stack.pop(), Some(2));
    assert_eq!(stack.pop(), Some(1));
    assert_eq!(stack.pop(), None); // Stack should be empty
}

#[test]
fn push_pop_multi_thread() {
    let stack = ElimStack::default();

    scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..4 {
            let handle = scope.spawn(|| {
                for i in 0..5_000 {
                    stack.push(i);
                    assert!(stack.pop().is_some());
                }
            });
            handles.push(handle);
        }
    });

    assert!(stack.pop().is_none());
}

#[test]
fn pop_empty_stack() {
    let stack: ElimStack<i32> = ElimStack::default();
    assert_eq!(stack.pop(), None); // Popping an empty stack should return None
}

#[test]
fn stress_test() {
    let stack = ElimStack::default();

    scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..10 {
            let handle = scope.spawn(|| {
                for i in 0..10_000 {
                    stack.push(i);
                }
            });
            handles.push(handle);
        }
    });

    let mut count = 0;
    while stack.pop().is_some() {
        count += 1;
    }
    assert_eq!(count, 100_000);
}

#[test]
fn concurrent_push_pop() {
    let stack: ElimStack<i32> = ElimStack::default();
    let count = AtomicI32::new(0);

    scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..1_000 {
            let handle = scope.spawn(|| {
                for i in 0..1_000 {
                    stack.push(i);
                }
            });
            handles.push(handle);
        }

        for _ in 0..5 {
            let handle = scope.spawn(|| {
                while count.load(Ordering::Acquire) < 1_000_000 {
                    if let Some(_) = stack.pop() {
                        count.fetch_add(1, Ordering::Release);
                    }
                }
            });
            handles.push(handle);
        }
    });

    assert!(stack.pop().is_none());
}
