# open-address-table
open address table using robin-hood hashing and linear probing in c++ 

performance using 64 bit keys vs <std::unordered_map>

<img width="1071" alt="image" src="https://github.com/user-attachments/assets/fdc2b7ab-df46-4c13-94b3-8fed19c92ae3">

- it looks like <std::unordered_map> is still faster when we mix operations which is to be expected, however my version does outperform when we test each operation individually


