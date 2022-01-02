library(R6)
library(stringr)
library(yaml)
library(RSQLite)

DataLoader <- R6Class("DataLoader",
  private = list(
    .partition = "",
    config = read_yaml("~/.rodpscache/conf/config.yaml")
  ),
  active = list(
    partition = function(value){
      if(missing(value)){
        private$.partition
      } else {
        stop("$partition is read only", call. = FALSE)
      }
    }
  ),
  public = list(
    initialize = function(partition){
      stopifnot(str_detect(partition, "^[0-9]{8}$"))
      private$.partition <- partition
    },
    clearCache = function(key){
      sapply(list.files(private$config[["cachedir"]], pattern = str_c(key, "*"), full.names = TRUE), unlink)
    },
    showConfig = function(x){
      private$config
    },
    cleverLoad = function(key){
      if(!(key %in% unlist(str_split(private$config[["keys"]], ",")))){
        stop(str_c("illegal Key: ", key))
      }
      csv_fn <- str_c(private$config[["cachedir"]], key, "_", private$.partition, ".csv")
      if(file.exists(csv_fn)){
        sdf <- read.csv(csv_fn)
        printf(str_c("Read ", key, "_", private$.partition, " from cached files"))
        reture(sdf)
      }

      sql <- sprintf(prviate$config[[key]], private$.partition)
      # TODO: add sql query

      write.csv(sdf, file=csv_fn)
      return(sdf)
    },
    print = function(...){
      cat("DataLoader: \n")
      cat(" Partition: ", private$.partition, "\n")
      cat(" Cache Keys: ", private$config[["keys"]])
      cat(" Cache Dir: ", private$config[["cachedir"]])
    }

  )
)
