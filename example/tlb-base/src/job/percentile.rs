// use std::sync::Once;

// ref: leveldb/util/histogram.cc
const SCALE: [f64; 90] = [
    1f64, 2f64, 3f64, 4f64, 5f64, 6f64, 7f64, 8f64, 9f64, 10f64, 12f64, 14f64, 16f64, 18f64, 20f64,
    25f64, 30f64, 35f64, 40f64, 45f64, 50f64, 60f64, 70f64, 80f64, 90f64, 100f64, 120f64, 140f64,
    160f64, 180f64, 200f64, 250f64, 300f64, 350f64, 400f64, 450f64, 500f64, 600f64, 700f64, 800f64,
    900f64, 1000f64, 1200f64, 1400f64, 1600f64, 1800f64, 2000f64, 2500f64, 3000f64, 3500f64,
    4000f64, 4500f64, 5000f64, 6000f64, 7000f64, 8000f64, 9000f64, 10000f64, 12000f64, 14000f64,
    16000f64, 18000f64, 20000f64, 25000f64, 30000f64, 35000f64, 40000f64, 45000f64, 50000f64,
    60000f64, 70000f64, 80000f64, 90000f64, 100000f64, 120000f64, 140000f64, 160000f64, 180000f64,
    200000f64, 250000f64, 300000f64, 350000f64, 400000f64, 450000f64, 500000f64, 600000f64,
    700000f64, 800000f64, 900000f64, 1000000f64,
];

pub fn get_percentile_scale() -> &'static [f64] {
    SCALE.as_ref()
}

// pub fn get_percentile_scale0() -> &'static [f64] {
//     static mut SCALE: Option<Vec<f64>> = None;
//     static INIT: Once = Once::new();
//     INIT.call_once(|| {
//         let mut scale = Vec::with_capacity(360);
//         for i in 0..50 {
//             scale.push((i + 1) as f64);
//         }
//
//         for i in (50..100).step_by(2) {
//             scale.push((i + 2) as f64);
//         }
//
//         for i in (100..1000).step_by(10) {
//             scale.push((i + 10) as f64);
//         }
//
//         for i in (1000..10000).step_by(100) {
//             scale.push((i + 100) as f64);
//         }
//
//         for i in (10000..100000).step_by(1000) {
//             scale.push((i + 1000) as f64);
//         }
//
//         unsafe { SCALE = Some(scale) }
//     });
//
//     unsafe { SCALE.as_ref().unwrap().as_slice() }
// }

#[cfg(test)]
mod tests {
    use crate::job::percentile::get_percentile_scale;
    use rlink::functions::percentile::Percentile;

    #[test]
    pub fn percentile_test() {
        // let a = SCALE[89] as u64;
        let mut percentile_buffer = [
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0,
            0, 0, 2,
        ];

        let percentile = Percentile::new(get_percentile_scale(), percentile_buffer.as_mut());

        let pct_99 = percentile.get_result(99);
        println!("{}", pct_99);
        println!("{}", pct_99 as u64);
    }
}