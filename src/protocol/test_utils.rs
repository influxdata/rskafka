macro_rules! test_roundtrip {
    ($t:ty, $name:ident) => {
        #[allow(unused_imports)]
        use proptest::prelude::*;

        proptest! {
            #![proptest_config(ProptestConfig{cases: 100, fork: false, ..Default::default()})]
            #[test]
            fn $name(orig: $t) {
                #[allow(unused_imports)]
                use std::io::Cursor;

                let mut buf = Cursor::new(Vec::<u8>::new());
                match orig.write(&mut buf) {
                    Err(_) => {
                        // skip
                    }
                    Ok(()) => {
                        let l = buf.position();
                        buf.set_position(0);

                        let restored = <$t>::read(&mut buf).unwrap();
                        assert_eq!(orig, restored);

                        assert_eq!(buf.position(), l);
                    }
                }
            }
        }
    };
}

pub(crate) use test_roundtrip;
