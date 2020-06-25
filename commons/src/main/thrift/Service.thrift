namespace * com.lsm.thrift

exception MyException {
    1: string   message,
    2: i16      code,
}

service Service {
   void put(1: string key, 2: i32 value) throws (1: MyException error);
   i32 get(1: string key) throws (1: MyException error);
   i32 delete(1: string key)
}
