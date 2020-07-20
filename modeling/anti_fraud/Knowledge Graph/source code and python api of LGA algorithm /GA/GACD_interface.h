//
//  GACD_interface.h
//  GA
//
//  Created by William Woo on 6/5/19.
//  Copyright © 2019 William Woo. All rights reserved.
//

#ifndef GACD_interface_h
#define GACD_interface_h

/* Include files */
#include <stddef.h>
#include <stdlib.h>
#include "rtwtypes.h"
#include "GACD_types.h"
#include <vector>
using namespace std;

void GACD(double *b_W1, int size_w, int nval, double pop_size, double L, double p1,
                double *Q,
          double** label, int* size_L, double** x_1, int* size_x, double** y_1, int* size_y);//, int size_y);

#endif /* GACD_interface_h */
