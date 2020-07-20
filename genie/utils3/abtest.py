import numpy as np
from statsmodels.stats.power import NormalIndPower

print("请输入实验主要指标当前值 ？%（比如点击率，留存率）")
u=input()
u=float(u)/100
print("请输入最小可以观测的提升比例 ？% （相对值）")
r=input()
r=float(r)/100

#计算单个实验组最小人数
def main(al='two-sided'):
    zpower = NormalIndPower()
    effect_size =u*r/np.sqrt(u*(1-u))
    print(zpower.solve_power(
        effect_size=effect_size,
        nobs1=None,
        alpha=0.1,
        power=0.8,
        ratio=1,
        alternative=al
    ))

main('larger')
