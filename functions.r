load_packages <- function(packages) {
  new_packages <- setdiff(packages, installed.packages()[, "Package"])
  
  if (length(new_packages) > 0) {
    install.packages(new_packages, repos = "https://cran.rstudio.com")
  }
  quiet <- sapply(packages, require, character.only = T, quietly = T)
}

read_sql <- function(fileName) {
  return(readChar(paste0(getwd(),'/sql/',fileName), file.info(paste0(getwd(),'/sql/',fileName))$size))
} 

