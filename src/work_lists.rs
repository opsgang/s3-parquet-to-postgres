use anyhow::Result;
use log::{debug, error, info};
use std::error::Error;
use std::fmt;
use std::fs::OpenOptions;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::Path;

#[derive(Debug)]
struct InconsistentWipFileToList;
impl fmt::Display for InconsistentWipFileToList {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "The current wip file contents don't match the wip list")
    }
}

impl Error for InconsistentWipFileToList {}

// Not a struct method as we call this before we return the initialised struct.
fn wip_file_to_wip_list(work_lists_dir: &str, num_items: usize) -> Result<Vec<String>> {
    // load wip into wip_list
    let mut wip_list: Vec<String> = Vec::with_capacity(num_items);
    let filename_wip = format!("{}/wip", work_lists_dir);
    if Path::new(filename_wip.as_str()).is_file() {
        info!("loading existing wip file at {}", filename_wip);
        let file_wip = OpenOptions::new().read(true).open(filename_wip.clone())?;

        // Read the wip file
        let reader = BufReader::new(file_wip);
        wip_list = reader
            .lines()
            .filter_map(|line| match line {
                Ok(line) if !line.trim().is_empty() && !line.trim_start().starts_with('#') => {
                    Some(Ok(line))
                }
                Ok(_) => None,
                Err(e) => Some(Err(e)),
            })
            .collect::<Result<_, _>>()?;
        debug!("wip_list from file: {:?}", wip_list);
    }
    Ok(wip_list)
}

// we control when to pull more items from todo with size of wip_list
// i.e. we remove from wip list when item is completed successfully
// Once wip list is empty, we know length is 0 and we can overwrite the wip file with new items
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkLists {
    pub batch_size: usize,

    pub work_lists_dir: String,
    pub filename_completed: String,
    pub filename_todo: String,
    pub filename_wip: String,

    pub wip_list: Vec<String>,
}

impl WorkLists {
    pub fn new(work_lists_dir: &str, num_items: usize) -> Result<Self> {
        // check todo file exists or nothing to do
        let filename_todo = format!("{}/todo", work_lists_dir);
        let filename_wip = format!("{}/wip", work_lists_dir);
        debug!("checking todo file at {}", filename_todo);
        {
            let _ = OpenOptions::new().read(true).open(filename_todo.clone())?;
        }

        let wip_list = wip_file_to_wip_list(work_lists_dir, num_items)?;
        info!("inital wip_list read from file: {:?}", wip_list);

        Ok(WorkLists {
            batch_size: num_items,
            work_lists_dir: work_lists_dir.to_string(),
            filename_completed: format!("{}/completed", work_lists_dir),
            filename_todo,
            filename_wip,
            wip_list,
        })
    }

    pub fn mark_completed(&mut self, completed_item: String) -> Result<()> {
        // remove item from wip_list
        debug!("... marking {} as completed", completed_item);
        self.wip_list.retain(|x| *x != completed_item);

        // FIRST add to completed file - we want to know it was done even if remaining steps fail
        let file_completed = OpenOptions::new()
            .create(true) // first run, won't exist, so this allows creation, but if exists, will reuse.
            .append(true)
            .open(self.filename_completed.clone())?;

        let mut completed_writer = io::BufWriter::new(file_completed);
        writeln!(completed_writer, "{}", completed_item)?;

        // 2. rewrite wip file without the item (was removed from self.wip_list)
        let file_wip = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(self.filename_wip.clone())?;

        let mut wip_writer = io::BufWriter::new(file_wip);
        for item in self.wip_list.clone() {
            writeln!(wip_writer, "{}", item)?;
        }
        Ok(())
    }

    pub fn next_batch(&mut self) -> Result<&mut Self> {
        if !self.wip_list.is_empty() {
            // should check that wip list file has same content or fail
            // as wip_list and file are out of sync
            let wip_list_from_file =
                wip_file_to_wip_list(self.work_lists_dir.as_str(), self.batch_size)?;
            if wip_list_from_file == self.wip_list {
                info!("wip list is not empty ... still have work in progress");
                return Ok(self);
            }

            error!("Contents of wip list from file do not match current wip list:");
            error!("File: {:?}", wip_list_from_file);
            error!("List: {:?}", self.wip_list);
            return Err(InconsistentWipFileToList.into());
        }

        debug!(
            "Taking first {} items from {}",
            self.batch_size, self.filename_wip
        );

        let file_todo = OpenOptions::new()
            .read(true)
            .create(false)
            .open(self.filename_todo.clone())?;

        // Read the next batch_size of lines (items) from the 'todo' file
        let mut reader = BufReader::new(file_todo);
        let mut items: Vec<String> = Vec::new();

        let lines_batched = reader
            .by_ref()
            .lines()
            .filter_map(|line| match line {
                Ok(line) if !line.trim().is_empty() && !line.trim_start().starts_with('#') => {
                    Some(Ok(line))
                }
                Ok(_) => None,
                Err(e) => Some(Err(e)),
            })
            .take(self.batch_size);

        for item in lines_batched {
            items.push(item?);
        }

        // Open the 'wip' file for writing
        debug!(
            "Writing next {} items to add to wip file: {}  ...",
            self.batch_size, self.filename_wip
        );
        let file_wip = OpenOptions::new()
            .write(true)
            .create(true) // first run, won't exist, so this allows creation, but if exists, will reuse.
            .truncate(true)
            .open(self.filename_wip.clone())?;

        // Write the first 5 lines to the 'wip' file
        let mut wip_writer = io::BufWriter::new(file_wip);
        for item in &items {
            writeln!(wip_writer, "{}", item)?;
        }

        // Collect remaining lines
        let remaining_items: Vec<String> = reader
            .lines()
            .filter_map(|line| match line {
                Ok(line) if !line.trim().is_empty() && !line.trim_start().starts_with('#') => {
                    Some(Ok(line))
                }
                Ok(_) => None,
                Err(e) => Some(Err(e)),
            })
            .collect::<Result<_, _>>()?;

        // Now reopen the 'todo' file for writing and truncate it
        debug!(
            "Open {}, truncated, for rewriting without items in progress ...",
            self.filename_todo
        );
        let mut file_todo = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(self.filename_todo.clone())?;

        // Write the remaining lines back to the 'todo' file
        debug!(
            "Writing remaining s3 files back to {}  ...",
            self.filename_todo
        );
        for item in remaining_items {
            writeln!(file_todo, "{}", item)?;
        }

        self.wip_list = items.clone();

        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_fs::{fixture::TempDir, prelude::*};
    use const_format::formatcp;
    use std::env;
    use std::fs;

    static TESTDATA_DIR: &str = formatcp!(
        "{}/{}",
        env!("CARGO_MANIFEST_DIR"),
        "tests/testdata/unit-tests/work_lists"
    );

    macro_rules! vec_stringify {
        ($($x:expr),*) => (vec![$($x.to_string()),*]);
    }

    fn work_lists_test_struct(
        batch_size: usize,
        work_lists_dir: String,
        wip_list: Vec<String>,
    ) -> WorkLists {
        WorkLists {
            batch_size,
            work_lists_dir: work_lists_dir.clone(),
            filename_completed: format!("{}/completed", work_lists_dir),
            filename_todo: format!("{}/todo", work_lists_dir),
            filename_wip: format!("{}/wip", work_lists_dir),
            wip_list,
        }
    }

    #[test]
    fn test_new_nonexistent_work_dir() {
        let result = WorkLists::new("non_existent_work_dir", 1);
        assert!(
            result.is_err(),
            "Expected an error when reading a non-existent file."
        );
    }

    #[test]
    fn test_new_nonexistent_todo_file() {
        let work_lists_dir = format!("{}/{}", TESTDATA_DIR, "empty_dir");
        let result = WorkLists::new(
            work_lists_dir.as_str(), // dir exists
            1,
        );
        assert!(
            result.is_err(),
            "Expected an error when reading a non-existent file."
        );
    }

    #[test]
    fn test_new_with_todo_file() {
        let work_lists_dir = format!("{}/{}", TESTDATA_DIR, "todo_only");
        let result = WorkLists::new(
            work_lists_dir.as_str(), // dir exists
            1,
        );
        let expected_wl: WorkLists = work_lists_test_struct(1, work_lists_dir.clone(), vec![]);

        assert!(
            result.is_ok(),
            "Should have read the todo file without issue"
        );

        let wl = result.unwrap();
        assert_eq!(expected_wl, wl);
    }

    #[test]
    fn test_new_with_wip_no_comments() {
        let work_lists_dir = format!("{}/{}", TESTDATA_DIR, "wip_no_whitespace_or_comments");

        let expected_wip_list: Vec<String> = vec!["ITEM_A".to_string(), "ITEM_B".to_string()];
        let expected_wl: WorkLists =
            work_lists_test_struct(2, work_lists_dir.clone(), expected_wip_list);

        let result = WorkLists::new(
            work_lists_dir.as_str(), // dir exists
            2,
        );
        assert!(
            result.is_ok(),
            "Should have read the todo file without issue"
        );

        let wl = result.unwrap();

        assert_eq!(expected_wl, wl);
    }

    #[test]
    fn test_new_reads_whole_wip_regardless_of_batch_size() {
        let work_lists_dir = format!("{}/{}", TESTDATA_DIR, "wip_no_whitespace_or_comments");

        let expected_wip_list: Vec<String> = vec!["ITEM_A".to_string(), "ITEM_B".to_string()];
        let expected_wl: WorkLists =
            work_lists_test_struct(1, work_lists_dir.clone(), expected_wip_list);

        let result = WorkLists::new(
            work_lists_dir.as_str(), // dir exists
            1, // there are 2 items in wip file, even though we only request 1 batch
        );

        assert!(
            result.is_ok(),
            "Should have read the todo and wip files without issue"
        );

        let wl = result.unwrap();

        assert_eq!(expected_wl, wl);
    }

    #[test]
    fn test_new_ignore_comments_and_whitespace_in_wip() {
        let work_lists_dir = format!("{}/{}", TESTDATA_DIR, "wip_with_whitespace_and_comments");

        let expected_wip_list: Vec<String> = vec!["ITEM_A".to_string(), "ITEM_B".to_string()];
        let expected_wl: WorkLists =
            work_lists_test_struct(2, work_lists_dir.clone(), expected_wip_list);

        let result = WorkLists::new(
            work_lists_dir.as_str(), // dir exists
            2,
        );
        assert!(
            result.is_ok(),
            "Should have read the todo and wip files without issue"
        );

        let wl = result.unwrap();
        assert_eq!(expected_wl, wl);
    }

    #[test]
    fn test_mark_completed_new_completed_file() {
        let tmp_dir = TempDir::new().unwrap();
        let src_dir = format!("{}/{}", TESTDATA_DIR, "mark_completed_new");
        tmp_dir.copy_from(src_dir.as_str(), &["*"]).unwrap();

        let work_lists_dir = format!("{}", tmp_dir.path().display());
        let wip_list: Vec<String> = vec!["apple".to_string(), "banana".to_string()];

        // We don't use new(), as we aren't unit testing that here.
        // need mut here as we'll call mark_completed(), a mutating method
        let mut wl: WorkLists = work_lists_test_struct(2, work_lists_dir.clone(), wip_list);

        // TEST THE METHOD
        let result = wl.mark_completed("apple".to_string());

        assert!(
            result.is_ok(),
            "should have rewritten wip and written new completed"
        );
        // compare completed file with expected
        let completed_file: String = fs::read_to_string(wl.filename_completed.as_str()).unwrap();
        let wip_file: String = fs::read_to_string(wl.filename_wip.as_str()).unwrap();
        tmp_dir.close().unwrap(); // can close file now

        assert_eq!(completed_file, "apple\n");
        assert_eq!(wip_file, "banana\n");
        assert_eq!(wl.wip_list, vec!["banana".to_string()]);
    }

    #[test]
    fn test_mark_completed_existing_completed_file() {
        let tmp_dir = TempDir::new().unwrap();
        let src_dir = format!("{}/{}", TESTDATA_DIR, "mark_completed_exists");
        tmp_dir.copy_from(src_dir.as_str(), &["*"]).unwrap();

        let work_lists_dir = format!("{}", tmp_dir.path().display());
        let wip_list: Vec<String> = vec!["cherry".to_string(), "dates".to_string()];

        // We don't use new(), as we aren't unit testing that here.
        // need mut here as we'll call mark_completed(), a mutating method
        let mut wl: WorkLists = work_lists_test_struct(2, work_lists_dir.clone(), wip_list);

        // TEST THE METHOD
        let result = wl.mark_completed("cherry".to_string());

        assert!(
            result.is_ok(),
            "should have updated wip and updated completed"
        );
        // compare completed file with expected
        let completed_file: String = fs::read_to_string(wl.filename_completed.as_str()).unwrap();
        let wip_file: String = fs::read_to_string(wl.filename_wip.as_str()).unwrap();
        tmp_dir.close().unwrap(); // can close file now

        assert_eq!(completed_file, "apple\nbanana\ncherry\n");
        assert_eq!(wip_file, "dates\n");

        assert_eq!(wl.wip_list, vec!["dates".to_string()]);
    }

    #[test]
    fn test_next_batch_existing_wip_list() {
        let tmp_dir = TempDir::new().unwrap();
        let src_dir = format!("{}/{}", TESTDATA_DIR, "wip_no_whitespace_or_comments");
        tmp_dir.copy_from(src_dir.as_str(), &["*"]).unwrap();

        // wip_list for this test should match contents of src_dir's wip file
        let work_lists_dir = format!("{}", tmp_dir.path().display());
        let wip_list: Vec<String> = vec!["ITEM_A".to_string(), "ITEM_B".to_string()];

        // We don't use new(), as we aren't unit testing that here.
        // need mut here as we'll call mark_completed(), a mutating method
        let mut wl: WorkLists = work_lists_test_struct(2, work_lists_dir.clone(), wip_list);

        // TEST THE METHOD
        let result = wl.next_batch();

        assert!(
            result.is_ok(),
            "wip file and list are consistent, so should have returned without modifying anything"
        );

        let current_wip_file: String = fs::read_to_string(wl.filename_wip.as_str()).unwrap();
        let current_todo_file: String = fs::read_to_string(wl.filename_todo.as_str()).unwrap();
        tmp_dir.close().unwrap(); // can close file now

        let src_wip_file: String = fs::read_to_string(format!("{}/wip", src_dir)).unwrap();
        let src_todo_file: String = fs::read_to_string(format!("{}/todo", src_dir)).unwrap();

        // check no files have changed
        assert_eq!(current_wip_file, src_wip_file);
        assert_eq!(current_todo_file, src_todo_file);
    }

    #[test]
    fn test_next_batch_inconsistent_wip_file_and_list() {
        let tmp_dir = TempDir::new().unwrap();
        let src_dir = format!("{}/{}", TESTDATA_DIR, "wip_no_whitespace_or_comments");
        tmp_dir.copy_from(src_dir.as_str(), &["*"]).unwrap();

        // wip_list for this test should NOT match contents of src_dir's wip file
        let work_lists_dir = format!("{}", tmp_dir.path().display());
        let wip_list: Vec<String> = vec!["NOT_ITEM_A".to_string(), "NOT_ITEM_B".to_string()];

        // We don't use new(), as we aren't unit testing that here.
        // need mut here as we'll call mark_completed(), a mutating method
        let mut wl: WorkLists = work_lists_test_struct(2, work_lists_dir.clone(), wip_list);

        // TEST THE METHOD
        let result = wl.next_batch();

        assert!(
            result.is_err(),
            "wip file and list should be inconsistent, so should error"
        );

        // check todo files has not changed - in inconsistent state, we don't want to suck up
        // more work from the todo!
        let current_todo_file: String = fs::read_to_string(wl.filename_todo.as_str()).unwrap();
        let src_todo_file: String = fs::read_to_string(format!("{}/todo", src_dir)).unwrap();
        tmp_dir.close().unwrap(); // can close file now

        assert_eq!(current_todo_file, src_todo_file);
    }

    #[test]
    fn test_next_batch_first_run() {
        let tmp_dir = TempDir::new().unwrap();
        let src_dir = format!("{}/{}", TESTDATA_DIR, "todo_only");
        tmp_dir.copy_from(src_dir.as_str(), &["*"]).unwrap();

        // We don't use new(), as we aren't unit testing that method.
        // need mut here as we'll call next_batch(), a mutating method
        let work_lists_dir = format!("{}", tmp_dir.path().display());
        let mut wl: WorkLists = work_lists_test_struct(3, work_lists_dir.clone(), vec![]);

        // TEST THE METHOD
        let result = wl.next_batch();

        assert!(result.is_ok(), "should have taken the top 3 items");

        let todo_file: String = fs::read_to_string(wl.filename_todo.as_str()).unwrap();
        let wip_file: String = fs::read_to_string(wl.filename_wip.as_str()).unwrap();
        tmp_dir.close().unwrap(); // can close file now

        assert_eq!(wl.wip_list, vec_stringify!["item_A", "item_B", "item_C"]);
        assert_eq!(todo_file, "item_D\nitem_E\n");
        assert_eq!(wip_file, "item_A\nitem_B\nitem_C\n");
    }

    #[test]
    fn test_next_batch_bigger_batch_than_todo() {
        let tmp_dir = TempDir::new().unwrap();
        let src_dir = format!("{}/{}", TESTDATA_DIR, "todo_only");
        tmp_dir.copy_from(src_dir.as_str(), &["*"]).unwrap();

        // We don't use new(), as we aren't unit testing that method.
        // need mut here as we'll call next_batch(), a mutating method
        let work_lists_dir = format!("{}", tmp_dir.path().display());
        let mut wl: WorkLists = work_lists_test_struct(10, work_lists_dir.clone(), vec![]);

        // TEST THE METHOD
        let result = wl.next_batch();

        assert!(
            result.is_ok(),
            "should have consumed all items as less than batch size"
        );

        let todo_file: String = fs::read_to_string(wl.filename_todo.as_str()).unwrap();
        let wip_file: String = fs::read_to_string(wl.filename_wip.as_str()).unwrap();
        tmp_dir.close().unwrap(); // can close file now

        assert_eq!(
            wl.wip_list,
            vec_stringify!["item_A", "item_B", "item_C", "item_D", "item_E"]
        );
        assert_eq!(todo_file, "");
        assert_eq!(wip_file, "item_A\nitem_B\nitem_C\nitem_D\nitem_E\n");
    }

    #[test]
    fn test_next_batch_empty_wip_and_todo_has_been_deleted() {
        let tmp_dir = TempDir::new().unwrap();
        let src_dir = format!("{}/{}", TESTDATA_DIR, "empty_dir");
        tmp_dir.copy_from(src_dir.as_str(), &["*"]).unwrap();

        // We don't use new(), as we aren't unit testing that method.
        // need mut here as we'll call next_batch(), a mutating method
        let work_lists_dir = format!("{}", tmp_dir.path().display());
        let mut wl: WorkLists = work_lists_test_struct(2, work_lists_dir.clone(), vec![]);

        // TEST THE METHOD
        let result = wl.next_batch();
        tmp_dir.close().unwrap(); // can close file now

        assert!(result.is_err(), "should throw err as todo file is missing");
    }

    #[test]
    fn test_next_batch_returns_modified_worklist() {
        let tmp_dir = TempDir::new().unwrap();
        let src_dir = format!("{}/{}", TESTDATA_DIR, "todo_only");
        tmp_dir.copy_from(src_dir.as_str(), &["*"]).unwrap();

        // We don't use new(), as we aren't unit testing that method.
        // need mut here as we'll call next_batch(), a mutating method
        let work_lists_dir = format!("{}", tmp_dir.path().display());
        let mut wl: WorkLists = work_lists_test_struct(2, work_lists_dir.clone(), vec![]);
        // TEST THE METHOD
        let wl2 = wl.next_batch().unwrap();
        tmp_dir.close().unwrap(); // can close file now

        let exp_wl: WorkLists = work_lists_test_struct(
            2,
            work_lists_dir.clone(),
            vec_stringify!["item_A", "item_B"],
        );
        assert_eq!(*wl2, exp_wl);
    }
}
