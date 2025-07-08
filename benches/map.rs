use criterion::{Criterion, criterion_group, criterion_main};
use map::{open_addressing, swiss};
use std::{collections::HashMap as StdHashMap, hint};

const BENCH_SIZE: usize = 1000;

fn generate_data(size: usize) -> Vec<(String, i32)> {
    (0..size).map(|i| (format!("key{}", i), i as i32)).collect()
}

fn bench_insert(c: &mut Criterion) {
    let data = generate_data(BENCH_SIZE);

    let mut group = c.benchmark_group("insert");

    group.bench_function("open_addressing", |b| {
        b.iter(|| {
            let mut map = open_addressing::Map::new();
            for (key, value) in data.iter() {
                map.insert(hint::black_box(key.clone()), hint::black_box(*value));
            }
        })
    });

    group.bench_function("swiss", |b| {
        b.iter(|| {
            let mut map = swiss::Map::new();
            for (key, value) in data.iter() {
                map.insert(hint::black_box(key.clone()), hint::black_box(*value));
            }
        })
    });

    group.bench_function("std_hashmap", |b| {
        b.iter(|| {
            let mut map = StdHashMap::new();
            for (key, value) in data.iter() {
                map.insert(hint::black_box(key.clone()), hint::black_box(*value));
            }
        })
    });

    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let data = generate_data(BENCH_SIZE);
    let mut oa_map = open_addressing::Map::new();
    let mut swiss_map = swiss::Map::new();
    let mut std_map = StdHashMap::new();

    for (key, value) in data.iter() {
        oa_map.insert(key.clone(), *value);
        swiss_map.insert(key.clone(), *value);
        std_map.insert(key.clone(), *value);
    }

    let mut group = c.benchmark_group("get");

    group.bench_function("open_addressing", |b| {
        b.iter(|| {
            for (key, _) in data.iter() {
                oa_map.get(hint::black_box(key));
            }
        })
    });

    group.bench_function("swiss", |b| {
        b.iter(|| {
            for (key, _) in data.iter() {
                swiss_map.get(hint::black_box(key));
            }
        })
    });

    group.bench_function("std_hashmap", |b| {
        b.iter(|| {
            for (key, _) in data.iter() {
                std_map.get(hint::black_box(key));
            }
        })
    });

    group.finish();
}

criterion_group!(benches, bench_insert, bench_get);
criterion_main!(benches);
