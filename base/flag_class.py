import numpy as np


class FLAG_LOIS():

    def __init__(self, flagMasks, flagMeanings):
        self.maskValues = flagMasks
        self.maskNames = flagMeanings.split(' ')

    def is_flag_valid(self, flag_name, value):
        value_f = self.maskValues[self.maskNames.index(flag_name)]
        return value_f == value

    def is_any_flag_valid(self,flag_names,value):
        for flag_name in flag_names:
            if self.is_flag_valid(flag_name,value):
                return True
        return False

    def Code(self, maskList):
        myCode = np.int8(0)
        for flag in maskList:
            myCode |= self.maskValues[self.maskNames.index(flag)]
        return myCode

    def Mask(self, flags, maskList):
        myCode = self.Code(maskList)
        flags = np.int8(flags)
        # print(myCode,flags)
        # print flags
        # print myCode
        return np.bitwise_and(flags, myCode)

    def Decode(self, val):
        count = 0
        res = []
        mask = np.zeros(len(self.maskValues))
        for value in self.maskValues:
            if value & val:
                res.append(self.maskNames[count])
                mask[count] = 1
            count += 1
        return (res, mask)
