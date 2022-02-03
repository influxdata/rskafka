//! Helper to build a vector w/o blowing up memory.

/// Default block size (10MB).
pub const DEFAULT_BLOCK_SIZE: usize = 1024 * 1024 * 10;

/// Helper to build a vector w/ limited memory consumption.
///
/// For small vectors the elements are not copied. For larger vectors the elements are only copied once (in contrast to
/// `O(log n)` when using a normal vector w/o pre-allocation).
#[derive(Debug)]
pub struct VecBuilder<T> {
    elements_per_block: usize,
    blocks: Vec<Vec<T>>,
    remaining_elements: usize,
}

impl<T> VecBuilder<T> {
    /// Create new builder.
    ///
    /// # Panic
    /// Pancis when block size is too small to hold a single element of `T`.
    /// Create new builder.
    pub fn new(expected_elements: usize) -> Self {
        Self::new_with_block_size(expected_elements, DEFAULT_BLOCK_SIZE)
    }

    ///
    /// # Panic
    /// Panics when block size is too small to hold a single element of `T`.
    pub fn new_with_block_size(expected_elements: usize, block_size: usize) -> Self {
        let element_size = std::mem::size_of::<T>();
        let elements_per_block = if element_size == 0 {
            expected_elements
        } else {
            block_size / element_size
        };
        if elements_per_block == 0 {
            panic!("Block size too small for this type!");
        }

        Self {
            elements_per_block,
            blocks: vec![Vec::with_capacity(
                elements_per_block.min(expected_elements),
            )],
            remaining_elements: expected_elements,
        }
    }

    /// Push new element to builder
    ///
    /// # Panic
    /// Panics when pushing more elements than got specific during builder creation.
    pub fn push(&mut self, element: T) {
        assert!(
            self.remaining_elements > 0,
            "Got more elements than expected!"
        );

        let mut target_block = self.blocks.last_mut().expect("Has always at least 1 block");
        if target_block.len() >= self.elements_per_block {
            self.blocks.push(Vec::with_capacity(
                self.remaining_elements.min(self.elements_per_block),
            ));
            target_block = self.blocks.last_mut().expect("Just pushed a new block");
        }
        target_block.push(element);
        self.remaining_elements -= 1;
    }
}

impl VecBuilder<u8> {
    /// Read as many bytes as there are left.
    pub fn read_exact<R>(mut self, reader: &mut R) -> Result<Self, std::io::Error>
    where
        R: std::io::Read,
    {
        // Note: We can modify `self` here and still return an error because there is no way the taken/moved `self` will
        //       be accessible in the error case.

        while self.remaining_elements > 0 {
            let mut buf = self.blocks.last_mut().expect("Has always at least 1 block");
            if buf.len() >= self.elements_per_block {
                self.blocks.push(Vec::with_capacity(
                    self.remaining_elements.min(self.elements_per_block),
                ));
                buf = self.blocks.last_mut().expect("Just pushed a new block");
            }

            let to_read = self
                .remaining_elements
                .min(self.elements_per_block - buf.len());

            let buf_start_pos = buf.len();
            buf.resize(buf.len() + to_read, 0);

            reader.read_exact(&mut buf[buf_start_pos..])?;
            self.remaining_elements -= to_read;
        }

        Ok(self)
    }
}

impl<T> From<VecBuilder<T>> for Vec<T> {
    fn from(builder: VecBuilder<T>) -> Self {
        if builder.blocks.len() == 1 {
            builder
                .blocks
                .into_iter()
                .next()
                .expect("Just checked number of blocks")
        } else {
            let mut out = Self::with_capacity(builder.blocks.iter().map(Self::len).sum());
            for mut block in builder.blocks.into_iter() {
                out.append(&mut block);
            }
            out
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_build() {
        let expected_elements = 7;
        let mut builder = VecBuilder::<i16>::new_with_block_size(expected_elements, 5);

        let mut expected = vec![];
        for i in 0..expected_elements {
            let i = i as i16;
            builder.push(i);
            expected.push(i);
        }

        let blocks = vec![vec![0, 1], vec![2, 3], vec![4, 5], vec![6]];
        assert_eq!(builder.blocks, blocks);

        let actual: Vec<_> = builder.into();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_zst() {
        let expected_elements = 3;
        let mut builder = VecBuilder::<()>::new_with_block_size(expected_elements, 1);

        let mut expected = vec![];
        for _ in 0..expected_elements {
            builder.push(());
            expected.push(());
        }

        let blocks = vec![vec![(), (), ()]];
        assert_eq!(builder.blocks, blocks);

        let actual: Vec<_> = builder.into();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_reader() {
        let data = b"abc".to_vec();
        let mut reader = Cursor::new(data.clone());

        let mut builder = VecBuilder::<u8>::new_with_block_size(data.len(), 2);
        builder = builder.read_exact(&mut reader).unwrap();

        let blocks = vec![b"ab".to_vec(), b"c".to_vec()];
        assert_eq!(builder.blocks, blocks);

        let actual: Vec<_> = builder.into();
        assert_eq!(actual, data);
    }

    #[test]
    fn test_push_reader_interaction() {
        let data = b"bc".to_vec();
        let mut reader = Cursor::new(data.clone());

        let mut builder = VecBuilder::<u8>::new_with_block_size(data.len() + 1, 2);
        builder.push(b'a');
        builder = builder.read_exact(&mut reader).unwrap();

        let blocks = vec![b"ab".to_vec(), b"c".to_vec()];
        assert_eq!(builder.blocks, blocks);

        let actual: Vec<_> = builder.into();
        assert_eq!(&actual, b"abc");
    }

    #[test]
    fn test_single_block_not_copied() {
        let expected_elements = 5;
        let mut builder = VecBuilder::<i16>::new_with_block_size(expected_elements, 10);

        let mut expected = vec![];
        for i in 0..expected_elements {
            let i = i as i16;
            builder.push(i);
            expected.push(i);
        }

        let blocks = vec![vec![0, 1, 2, 3, 4]];
        assert_eq!(builder.blocks, blocks);

        let addr = builder.blocks[0].as_ptr();

        let actual: Vec<_> = builder.into();
        assert_eq!(actual, expected);
        assert_eq!(actual.as_ptr(), addr);
    }

    #[test]
    #[should_panic(expected = "Block size too small for this type!")]
    fn test_panic_block_size_too_small() {
        VecBuilder::<i16>::new_with_block_size(0, 1);
    }

    #[test]
    #[should_panic(expected = "Got more elements than expected!")]
    fn test_panic_too_many_elements() {
        let mut builder = VecBuilder::<i8>::new_with_block_size(0, 1);
        builder.push(1);
    }
}
