class Solution:
    def addBinary(self, a: str, b: str) -> str:

        added = int(a, 2) + int(b, 2)

        bit_length = added.bit_length()

        result = ""
        result.startswith()
        result.find()
        for index, bit in enumerate(bit_length):
            result += added % b

bin
        return result

print(Solution().addBinary('001', '011'))

print(int('1110', base=2).bit_length())