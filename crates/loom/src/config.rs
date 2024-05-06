use std::env;
use std::fs::File;
use std::io::Read;
use log::{debug, info};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: Server,
    pub cache: Cache,
    pub consumer: ConsumerKind,
    pub market: Market,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Server {
    pub port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cache {
    pub backend: Option<CacheBackend>,
    pub redis: RedisCache,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CacheBackend {
    Redis
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisCache {
    pub host: String,
    pub port: Option<u16>,
    pub database: Option<u16>,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsumerKind {
    Console,
    Redis,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Market {
    pub symbols: Option<Vec<String>>,
}


pub const DEFAULT_CONFIG_ENV_VAR: &str = "LOOM_CONFIG_FILE";
pub const DEFAULT_CONFIG_FILE_PATH: &str = "/etc/loom/config.toml";

impl Config {
    pub fn from_file(file: Option<&str>) -> anyhow::Result<Config> {
        let path = match file {
            None => {
                // 尝试读取环境变量
                // debug!("尝试读取环境变量中的配置文件名");
                debug!("try read config file path in env args");
                match env::var(DEFAULT_CONFIG_ENV_VAR) {
                    Ok(file_path) => file_path,
                    Err(_) => DEFAULT_CONFIG_FILE_PATH.to_string()
                }
            }
            Some(path) => path.to_string()
        };
        info!("apply config file: {}", &path);
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let config = toml::from_str::<Config>(&contents)?;
        Ok(config)
    }
}

impl RedisCache {
    pub fn to_redis_uri(&self) -> String {
        let port = self.port.unwrap_or(6379);
        let db = self.database.unwrap_or(0);
        format!("redis://{}:{}/{}", self.host, port, db)
    }
}

#[cfg(test)]
mod test {
    use crate::config::Config;

    #[test]
    fn config_load_test() {
        let config = Config::from_file(Some("config.toml")).unwrap();
        println!("{:?}", &config);
        assert!(config.server.port.is_some())
    }
}