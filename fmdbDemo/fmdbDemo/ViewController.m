//
//  ViewController.m
//  fmdbDemo
//
//  Created by yaojian on 2022/3/10.
//

#import "ViewController.h"
#import <FMDB/FMDB.h>
#import <sqlite3.h>
#import "UIView+Frame.h"

typedef void(^callBack)(FMDatabase *db);

@interface ViewController ()
@property(nonatomic, strong) FMDatabase *db;
@end

@implementation ViewController

- (void)viewDidLoad {
    [super viewDidLoad];
    // Do any additional setup after loading the view.
    CGFloat margin = 15;
    UIButton *addbtn = [self getButtonWithTitle:@"insert data" selector:@selector(queueInsert) offsetY:0];
    [self.view addSubview:addbtn];
    UIButton *addbtn1 = [self getButtonWithTitle:@"insert by tranction" selector:@selector(transactionInsert) offsetY:0];
    [self.view addSubview:addbtn1];
    addbtn1.left = addbtn.right + margin;
    
    UIButton *deletebtn = [self getButtonWithTitle:@"delete data" selector:@selector(delete) offsetY:60*1];
    [self.view addSubview:deletebtn];
    UIButton *updatebtn = [self getButtonWithTitle:@"update data" selector:@selector(update) offsetY:60*1];
    [self.view addSubview:updatebtn];
    updatebtn.left = deletebtn.right + margin;
    
    UIButton *selectbtn = [self getButtonWithTitle:@"select data" selector:@selector(select) offsetY:60*2];
    [self.view addSubview:selectbtn];
    UIButton *addcolumnbtn = [self getButtonWithTitle:@"add column" selector:@selector(addColum) offsetY:60*2];
    [self.view addSubview:addcolumnbtn];
    addcolumnbtn.left = selectbtn.right + margin;
    
    UIButton *addIndexBtn = [self getButtonWithTitle:@"create index" selector:@selector(createIndex) offsetY:60*3];
    [self.view addSubview:addIndexBtn];
    UIButton *selectbtn1 = [self getButtonWithTitle:@"select data by index" selector:@selector(selectWithIndex) offsetY:60*3];
    [self.view addSubview:selectbtn1];
    selectbtn1.left = addIndexBtn.right + margin;
    
    [self createDataBase];
}

- (UIButton *)getButtonWithTitle:(NSString *)title selector:(SEL)selector offsetY:(CGFloat)offsetY {
    CGFloat margin = 15;
    CGFloat screenWidth = [UIScreen mainScreen].bounds.size.width;
    CGFloat width = (screenWidth - margin*3) / 2;
    UIButton *btn = [[UIButton alloc] initWithFrame:CGRectMake(margin, 100 + offsetY, width, 50)];
    btn.backgroundColor = [UIColor colorWithRed:17/255.0 green:185/255.0 blue:85/255.0 alpha:1.0];
    [btn setTitle:title forState:UIControlStateNormal];
    [btn setTitleColor:UIColor.blackColor forState:UIControlStateNormal];
    btn.layer.cornerRadius = 5;
    btn.layer.masksToBounds = YES;
    [btn addTarget:self action:selector forControlEvents:UIControlEventTouchUpInside];
    return btn;
}

- (void)createDataBase {
    NSString *dbPath = [NSTemporaryDirectory() stringByAppendingPathComponent:@"mydata.db"];
    NSLog(@"dbPath-> %@", dbPath);
    
    NSFileManager *fileManager = [NSFileManager defaultManager];
    if (![fileManager fileExistsAtPath:dbPath]) {
        FMDatabase *db = [FMDatabase databaseWithPath:dbPath];
        self.db = db;
        if (![db open]) {
            NSLog(@"can not open the database");
            db = nil;
            return;
        }
        
        [_db executeUpdate:@"create table if not exists student (id integer primary key autoincrement, name text, age integer, height double, score integer, date double)"];
    }
}

- (FMDatabaseQueue *)FMDatabaseQueue {
    NSString *dbPath = [NSTemporaryDirectory() stringByAppendingPathComponent:@"mydata.db"];
    FMDatabaseQueue *dbQueue = [FMDatabaseQueue databaseQueueWithPath:dbPath];
    return dbQueue;
}

- (void)_insertInDB:(FMDatabase *)db {
    __block NSInteger i = 0;
    NSTimeInterval startTime = [[NSDate date] timeIntervalSince1970];
    NSLog(@"start_time-------%f", startTime);
    while (i++ < 100000) {
        [db executeUpdate:@"insert into student (name, age, height, score, date) values (?, ?, ?, ?, ?)",
         @"John",
         @(arc4random()%(30 -20 + 1) + 20),
         @((arc4random()%(17 -15 + 1) + 15)*0.1),
         @(arc4random()%(90 -50 + 1) + 50),
         [NSDate date]];
        NSLog(@"insert-------%ld", (long)i);
    }

    NSTimeInterval endTime = [[NSDate date] timeIntervalSince1970];
    NSLog(@"end_time-------%f", endTime);
    NSLog(@"used_time-------%f", endTime - startTime);
}

- (void)_excuteSqlByQueue:(NSString *)sql {
    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
        NSTimeInterval startTime = [[NSDate date] timeIntervalSince1970];
        NSLog(@"start_time-------%f", startTime);

        FMDatabaseQueue *dbQueue = [self FMDatabaseQueue];
        [dbQueue inDatabase:^(FMDatabase * _Nonnull db) {
            [db executeUpdate:sql];
        }];
        
        NSTimeInterval endTime = [[NSDate date] timeIntervalSince1970];
        NSLog(@"end_time-------%f", endTime);
        NSLog(@"used_time-------%f", endTime - startTime);
    });
}

- (void)_excuteSqlByTransaction:(NSString *)sql {
    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
        
        NSTimeInterval startTime = [[NSDate date] timeIntervalSince1970];
        NSLog(@"start_time-------%f", startTime);

        FMDatabaseQueue *dbQueue = [self FMDatabaseQueue];
        [dbQueue inTransaction:^(FMDatabase * _Nonnull db, BOOL * _Nonnull rollback) {
            [db executeUpdate:sql];
        }];
        
        NSTimeInterval endTime = [[NSDate date] timeIntervalSince1970];
        NSLog(@"end_time-------%f", endTime);
        NSLog(@"used_time-------%f", endTime - startTime);
    });
}

- (void)_selectByQueue:(NSString *)sql {
    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
        NSTimeInterval startTime = [[NSDate date] timeIntervalSince1970];
        NSLog(@"start_time-------%f", startTime);

        FMDatabaseQueue *dbQueue = [self FMDatabaseQueue];
        [dbQueue inDatabase:^(FMDatabase * _Nonnull db) {
            db.shouldCacheStatements = YES;
            FMResultSet *res = [db executeQuery:sql];
            while (res.next) {
                NSString *class = [res stringForColumn:@"class"];
                NSInteger ID = [res intForColumn:@"id"];
                NSString *name = [res stringForColumn:@"name"];
                NSInteger age = [res intForColumn:@"age"];
                CGFloat height = [res doubleForColumn:@"height"];
                NSInteger score = [res intForColumn:@"score"];
                NSLog(@"class= %@,ID=%ld, name=%@, age=%ld, height=%f, score=%ld",class, (long)ID, name, (long)age, height, (long)score);
            }
        }];
        
        NSTimeInterval endTime = [[NSDate date] timeIntervalSince1970];
        NSLog(@"end_time-------%f", endTime);
        NSLog(@"used_time-------%f", endTime - startTime);
    });
}

- (void)_selectByTransaction:(NSString *)sql {
    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
        NSTimeInterval startTime = [[NSDate date] timeIntervalSince1970];
        NSLog(@"start_time-------%f", startTime);

        FMDatabaseQueue *dbQueue = [self FMDatabaseQueue];
        [dbQueue inTransaction:^(FMDatabase * _Nonnull db, BOOL * _Nonnull rollback) {
            db.shouldCacheStatements = YES;
            FMResultSet *res = [db executeQuery:sql];
            while (res.next) {
                NSString *class = [res stringForColumn:@"class"];
                NSInteger ID = [res intForColumn:@"id"];
                NSString *name = [res stringForColumn:@"name"];
                NSInteger age = [res intForColumn:@"age"];
                CGFloat height = [res doubleForColumn:@"height"];
                NSInteger score = [res intForColumn:@"score"];
                NSLog(@"class= %@,ID=%ld, name=%@, age=%ld, height=%f, score=%ld",class, (long)ID, name, (long)age, height, (long)score);
            }
        }];
        
        NSTimeInterval endTime = [[NSDate date] timeIntervalSince1970];
        NSLog(@"end_time-------%f", endTime);
        NSLog(@"used_time-------%f", endTime - startTime);
    });
}

#pragma mark - insert
- (void)queueInsert {
    __weak typeof(self) weakSelf = self;
    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
        FMDatabaseQueue *dbQueue = [self FMDatabaseQueue];
        [dbQueue inDatabase:^(FMDatabase * _Nonnull db) {
            [weakSelf _insertInDB:db];
        }];
    });
}

- (void)transactionInsert {
    __weak typeof(self) weakSelf = self;
    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
        FMDatabaseQueue *dbQueue = [self FMDatabaseQueue];
        [dbQueue inTransaction:^(FMDatabase * _Nonnull db, BOOL * _Nonnull rollback) {
            [weakSelf _insertInDB:db];
        }];
    });
}

#pragma mark - delete
- (void)delete {
    [self _excuteSqlByQueue:@"delete from student where id > 0"]; // 0.045585
//    [self _excuteSqlByTransaction:@"delete from student where id >= 820034"]; // 0.023983
}


#pragma mark - update
- (void)update {
    [self _excuteSqlByQueue:@"update student set score = 100 where id = 95000"]; // 0.055596
//    [self _excuteSqlByTransaction:@"delete from student where id >= 420034"]; // 0.071845
}

#pragma mark - select
- (void)select {
    [self _selectByQueue:@"select 'select' as class, id, name, age, height, score from student where score = 100"]; // 0.768000
//    [self _selectByTransaction:@"select 'select' as class, id, name, age, height, score from student where id > 80000"]; // 0.768000

}

- (void)addColum {
    [self _excuteSqlByQueue:@"alter table student add column other"];
}

#pragma -mark - index
- (void)createIndex {
    [self _excuteSqlByQueue:@"create index score_index on student (score)"];
}

- (void)selectWithIndex {
    [self _selectByQueue:@"select 'insert-index' as class, id, name, age, height, score from student indexed by score_index where score = 100"];
}


@end
