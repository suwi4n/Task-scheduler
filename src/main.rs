use std::{sync::{Arc, Mutex}, time::Duration};
use serde::{Serialize, Deserialize};
use std::fs;

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
    let path = "tasks.json";

    let test_task = Task {
        task_id: 1,
        command: "notepad.exe".to_string(),
        args: vec![],
        trigger: Trigger::Interval(60),
    };

    let tasks_to_save = vec![test_task];

    println!("Сохраняем задачу в файл {}...", path);
    save_task(&path, &tasks_to_save)?;
    println!("Файл успешно сохранен");

    println!("Читаем задачи из файла");
    let loaded_tasks = load_task(&path)?;
    println!("Успешно прочитано задач {}", loaded_tasks.len());
    println!("Данные задачи {:#?}", loaded_tasks[0]);

    Ok(())
}