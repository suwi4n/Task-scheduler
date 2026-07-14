use core::task;
use std::{thread::current, time::Duration};
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use tokio::{fs, sync::Mutex};
use chrono::{DateTime, Utc};


#[derive(Debug, Clone, Serialize, Deserialize)]
enum Trigger {
    Interval(u64),
    Once,
    Cron(String),
    SystemEvent(String),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub task_id: u32,
    pub command: String,
    pub args: Vec<String>,
    pub trigger: Trigger,
    #[serde(default)]
    pub next_run: Option<DateTime<Utc>>,
}

pub async fn load_task(path: &str) -> Result<Vec<Task>, Box<dyn std::error::Error>> {
    let tasks: Vec<Task> = serde_json::from_str(&fs::read_to_string(path).await?)?;
    Ok(tasks)
}
pub async fn save_task(path: &str, tasks: &Vec<Task>) -> Result<(), Box<dyn std::error::Error>> {
    let writing = fs::write(path, serde_json::to_string_pretty(tasks)?).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let path = "tasks.json";

    let mut init_tasks = load_task(path).await?;
    let now = Utc::now();
    for task in &mut init_tasks {
        match task.trigger {
            Trigger::Interval(secs) => {
                task.next_run = Some(now + chrono::Duration::seconds(secs as i64));
            }
            Trigger::Once => {
                if task.next_run.is_none() {
                    task.next_run = Some(now);
                }
            }
            _ => {}
        }
    }
    let tasks = Arc::new(Mutex::new(init_tasks));
    println!("Планировщик запущен");

    loop {
        let current_time = Utc::now();
        let mut tasks_guard = tasks.lock().await;
        let mut list_changed = false;

        let mut i = 0;
        while i < tasks_guard.len() {
            let mut run_task = false;
            let mut sc_time = Utc::now();

            if let Some(run_time) = tasks_guard[i].next_run {
                if current_time >= run_time {
                    run_task = true;
                    sc_time = run_time;
                }
            }
            if run_task {
                let task = tasks_guard[i].clone();

                let mut should_remove = false; 
                match task.trigger {
                    Trigger::Once => {
                        should_remove = true;
                    },
                    Trigger::Interval(secs) => {
                        tasks_guard[i].next_run = Some(sc_time + chrono::TimeDelta::try_seconds(secs as i64).unwrap_or_default());
                        list_changed = true;
                    }
                    _ => {}
                };

            tokio::spawn(async move {
                let mut cmd = tokio::process::Command::new(&task.command);
                cmd.args(&task.args);

                println!("задача {} запущена в фоне", task.task_id);

                match cmd.spawn() {
                    Ok(mut child) => {
                        match child.wait().await {
                            Ok(status) => println!("задача {} завершилась: {}", task.task_id, status),
                            Err(e) => eprintln!("ошибка завершения задачи {}: {}", task.task_id, e),
                        }
                    }
                    Err(e) => eprintln!("не удалось запустит задачу {}: {}", task.task_id, e)
                }
            });
            if should_remove {
                tasks_guard.remove(i);
                list_changed = true;
                continue;
            }
            }
            i += 1;
        }
        if list_changed {
            save_task(path, &tasks_guard).await?;
        }
        drop(tasks_guard);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}