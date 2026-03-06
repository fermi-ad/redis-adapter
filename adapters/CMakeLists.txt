# Adapters - digital twins and signal processing
# Each subdirectory is a self-contained adapter with its own CMakeLists.txt
# To add a new adapter: create a subdirectory and add_subdirectory() here

add_subdirectory(device-twin)
add_subdirectory(baseline-subtract)
add_subdirectory(integrate)
add_subdirectory(filter)
add_subdirectory(magnitude)
add_subdirectory(position-intensity)
add_subdirectory(demux)
add_subdirectory(fft)
add_subdirectory(bpm)
add_subdirectory(bpm-twin)
add_subdirectory(blm)
add_subdirectory(blm-twin)
add_subdirectory(bcm)
add_subdirectory(bcm-twin)
