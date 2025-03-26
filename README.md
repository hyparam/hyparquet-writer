# Hyparquet Writer

[![mit license](https://img.shields.io/badge/License-MIT-orange.svg)](https://opensource.org/licenses/MIT)
![coverage](https://img.shields.io/badge/Coverage-96-darkred)

## Usage

```javascript
import { writeParquet } from 'hyparquet-writer'

const arrayBuffer = writeParquet({
  name: ['Alice', 'Bob', 'Charlie'],
  age: [25, 30, 35],
})
```

## References

 - https://github.com/hyparam/hyparquet
 - https://github.com/apache/parquet-format
 - https://github.com/apache/parquet-testing
