import numpy as np
import pandas as pd
from numpy import maximum, minimum
from cvxopt import matrix, solvers

solvers.options['show_progress'] = False


class Optimizer(object):

    def optimize(self, account):
        snap  = account.snap
        alpha = snap['Alpha'].values
        prc   = maximum(1.0, snap['Mid'])

        # Here we run a simple optimization
        trgt_pos  = 30 * alpha

        # all positions must be capped at $ 20 
        trgt_pos = minimum(trgt_pos,  20)
        trgt_pos = maximum(trgt_pos, -20)

        # @FixMe:
        # Here the interface is a bit confusing, improvement needed
        account._snap['PosTrgt' ] = trgt_pos / prc
        account._snap['PosFinal'] = np.round(trgt_pos / prc, 2)

        return account


class LinearOptimizer(Optimizer):

    def optimize(self, account):
        grouped = account.groupby('Exchange')
        dfs = []

        for key, df in grouped:
            mv    = np.sum(df['PosCurr'].values * df['Mid'])
            alpha = df['Alpha'].values
            prc   = maximum(1.0, df['Mid'])
            curr_mv = df['PosCurr'] * prc
            is_usd = df['Ccy'] == 'USD'
            restricted = is_usd | df['IsRestricted']

            n = len(df)
            # capital constraint
            # x[0] + ... + x[n] <= mv
            A = np.where(is_usd, np.zeros((1, n)), np.ones((1, n)))
            b = np.array([mv])
            # only allow long positions for now
            # x[0], ..., x[n] >= 0
            A = np.append(A, -np.eye(n), axis=0)
            b = np.append(b, np.where(restricted, -curr_mv, np.zeros(n)))
            # all positions must be capped at $30
            # x[0], ..., x[n] <= 30
            A = np.append(A, np.eye(n), axis=0)
            b = np.append(b, np.where(restricted, curr_mv, 30 * np.ones(n)))

            A = matrix(A)
            b = matrix(b)
            c = matrix(-alpha)

            # min c'x
            # s.t. Ax <= b
            sol=solvers.lp(c,A,b)

            trgt_mv = np.transpose(sol['x'])[0]
            df['PosTrgt'] = trgt_mv / prc
            df['PosFinal'] = np.round(trgt_mv / prc, 2)

            dfs.append(df)

        account._snap = pd.concat(dfs)
        return account
