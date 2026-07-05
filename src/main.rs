use std::{sync::{Arc, Mutex}, time::Duration};
use serde::{Serialize, Deserialize};
use std::fs;
use chrono::{DateTime, Utc};


#[derive(Debug, Clone, Serialize, Deserialize)]
enum Trigger {
    Interval(u64),
    Cron(String),
    SystemEvent(String),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Task {
    pub task_id: u32,
    pub command: String,
    pub args: Vec<String>,
    pub trigger: Trigger,
    #[serde(default)]
    pub next_run: Option<DateTime<Utc>>,
    #[serde(skip)]
    pub is_running: bool,
}

pub fn load_task(path: &str) -> Result<Vec<Task>, Box<dyn std::error::Error>> {
    let tasks: Vec<Task> = serde_json::from_str(&fs::read_to_string(path)?)?;
    Ok(tasks)
}
pub fn save_task(path: &str, tasks: &Vec<Task>) -> Result<(), Box<dyn std::error::Error>> {
    let writing = fs::write(path, serde_json::to_string_pretty(&tasks)?)?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(100);

    let path = "tasks.json";

    let mut tasks = load_task(&path)?;
    let now = Utc::now();
    for task in &mut tasks {
        if let Trigger::Interval(secs) = task.trigger {
            task.next_run = Some(now + chrono::Duration::seconds(secs as i64));
        }
    }
    println!("Планировщик запущен");

    loop {
        let current_time = Utc::now();

        for task in &mut tasks {
            if let Some(run_time) = task.next_run {
                if current_time >= run_time {
                    if task.is_running {
                        println!("Задача {} еще выполняется. скип", task.task_id);
                        task.next_runj = Some(Utc::now() + chrono::TimeDelta::try_seconds(1).unwrap_or_default());
                        continue;
                    }

                    task.is_running = true;
                    
                    let mut cmd = tokio::process::Command::new(&task.command);
                    cmd.args(&task.args);
                    cmd.stdout(Stdio::inherit());
                    cmd.stderr(Stdio::inherit());
                    match cmd.spawn() {
                        Ok(child) => {
                            println!("Процесс для задачи {} запущен", task.task_id)
                            /*let tx_clone = tx.clone();
                            let id = task.task_id;

                            tokio::spawn(async move {
                                let _ = child.wait().await;
                                let _ = tx_clone.send(id).await;
                            })*/
                        }
                        Err(e) => {
                            eprintln!("Не удалось запустить задачу '{}': {}", task.command, e); 
                            task.is_running = false;
                        }
                    }

                    if let Trigger::Interval(secs) = task.trigger {
                        task.next_run = Some(Utc::now() + chrono::TimeDelta::try_seconds(secs as i64).unwrap_or_default());
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}