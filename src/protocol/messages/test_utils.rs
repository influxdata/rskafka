macro_rules! test_roundtrip_versioned {
    ($t:ty, $min:expr, $max:expr, $name:ident) => {
        #[allow(unused_imports)]
        use proptest::prelude::*;

        proptest! {
            #![proptest_config(ProptestConfig{fork: false, ..Default::default()})]
            #[test]
            fn $name(orig: $t) {
                #[allow(unused_imports)]
                use std::io::Cursor;

                for v in ($min.0.0)..=($max.0.0) {
                    let v = ApiVersion(Int16(v));

                    let mut buf = Cursor::new(Vec::<u8>::new());
                    match orig.write_versioned(&mut buf, v) {
                        Err(_) => {
                            // skip
                        }
                        Ok(()) => {
                            buf.set_position(0);
                            let restored_1 = <$t>::read_versioned(&mut buf, v).unwrap();

                            // `orig` and `restored` might be different here, so we need another roundtrip
                            let mut buf = Cursor::new(Vec::<u8>::new());
                            restored_1.write_versioned(&mut buf, v).unwrap();

                            let l = buf.position();
                            buf.set_position(0);

                            let restored_2 = <$t>::read_versioned(&mut buf, v).unwrap();
                            assert_eq!(restored_1, restored_2);

                            assert_eq!(buf.position(), l);
                        }
                    }
                }
            }
        }
    };
}

pub(crate) use test_roundtrip_versioned;
