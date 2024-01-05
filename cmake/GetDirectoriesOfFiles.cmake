# TODO: put this somewhere where we can reuse it more broadly
# function to extract include directories from a list of files
# takes two arguments:
#   - _files: list of files
#   - _var: variable to store the list of include directories
#   - example usage get_directory_of_files("${REDIS_ADAPTER_HEADERS}" REDIS_ADAPTER_INCLUDE_DIRS)
#         where REDIS_ADAPTER_HEADERS contains the names of the headers used and
#         REDIS_ADAPTER_INCLUDE_DIRS is the name of the variable you want the result stored in
function(get_directories_of_files _files _var)
    # iterate over the list of files
    foreach(_file ${_files})
        # extract the directory of each header
        get_filename_component(_dir ${_file} DIRECTORY)
        # add the directory to the list of include directories
        list(APPEND directories ${_dir})
    endforeach()
    # remove duplicates from the list of include directories
    list(REMOVE_DUPLICATES directories)
    # set the variable to the parent scope so it can be used outside the function
    set(${_var} ${directories} PARENT_SCOPE)
endfunction()
