## 3.0.2: 2018/07/27

- throw proper errors in all cases
- updated README and helper function docs

## 3.0.1: 2018/07/24

- interpret ` ` as `undefined` (conforms to [spec](http://www.dbase.com/Knowledgebase/INT/db7_file_fmt.htm)), thx @donpedro!

## 3.0.0: 2018/07/16

- switched to class instantiation

## 2.2.0: 2018/06/02

- added pagination support (`offset` and `size` parameters)
- updated README for better explanation of options
- made `deleted` option parsing more strict
- got code coverage to 100%

## 2.1.0: 2018/05/31

- Add coverage tooling
- Added support for node 10
- Emit errors on duplicate field names
- Include records flagged deleted when `options.deleted` is `true`
- Added documentation things to package.json

## 2.0.0: 2018/05/30

- Operates as a transform stream

## 1.0.0: 2018/02/28

- Bad version, don't use, only emits events, doesn't transform as I had planned to do
