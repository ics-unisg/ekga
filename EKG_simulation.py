from datetime import datetime

class SimulationEKG:

    def __init__(self):
        self.steps = [
            (1, "Donor check-in", {"donor": "D2", "station": "1"}), # T = 1 Donor D2 checks in at R
            (2, "HCW check-in", {"hcw": "H1", "station": "3"}), # T = 2 HCW H1 checks in
            (3, "HCW check-in", {"hcw": "H2", "station": "3"}), # T = 3 HCW H2 checks in
            (4, "Hand hygiene", {"hcw": "H1", "station": "3"}), # T = 4 H1 performs hand hygiene
            (5, "Donor check-in", {"donor": "D1", "station": "2"}), # T = 5 Donor D1 checks in at L
            (6, "Hand hygiene", {"hcw": "H2", "station": "3"}), # T = 6 H2 performs hand hygiene
            (7, "Check blood drawing machine", {"hcw": "H1", "machine": "ML", "station": "2"}), # T = 7 H1 checks blood drawing machine ML at L
            (8, "Apply tourniquet", {"hcw": "H1", "donor": "D1", "station": "2"}), # T = 8 H1 applies tourniquet on D1 at L
            (9,"Hand hygiene", {"hcw": "H1", "station": "3"}), # T = 9 H1 performs hand hygiene
            (10, "Disinfect injection site", {"hcw": "H1", "donor": "D1", "station": "2"}), # T = 10 H1 disinfects injection site on D1 at L
            (11, "Perform venipuncture", {"hcw": "H1", "donor": "D1", "station": "2"}), # T = 11 H1 performs venipuncture on D1 at L
            (12, "Remove tourniquet", {"hcw": "H1", "donor": "D1", "station": "2"}), # T = 12 H1 removes tourniquet from D1 at L
            (13, "Activate blood drawing machine", {"hcw": "H1", "machine": "ML", "station": "2"}), # T = 13 H1 activates the blood drawing machine ML at L
            (14, "Hand hygiene", {"hcw": "H1", "station": "3"}), # T = 14 H1 performs hand hygiene
            (15, "Check blood drawing machine", {"hcw": "H1", "machine": "MR", "station": "1"}), # T = 15 H1 checks blood drawing machine MR at R
            (16, "Apply tourniquet", {"hcw": "H2", "donor": "D2", "station": "1"}), # T = 16 H2 applies tourniquet on D2 at R
            (17, "Hand hygiene", {"hcw": "H1", "station": "3"}), # T = 17 H1 performs hand hygiene
            (18, "Disinfect injection site", {"hcw": "H2", "donor": "D2", "station": "1"}), # T = 18 H2 disinfects injection site on D2 at R
            (19, "Perform venipuncture", {"hcw": "H2", "donor": "D2", "station": "1"}), # T = 19 H2 performs venipuncture on D2 at R
            (20, "Remove tourniquet", {"hcw": "H2", "donor": "D2", "station": "1"}), # T = 20 H2 removes tourniquet from D2 at R
            (21, "Activate blood drawing machine", {"hcw": "H2", "machine": "MR", "station": "1"}), # T = 21 H2 activates the blood drawing machine MR at R
            (22, "Hand hygiene", {"hcw": "H2", "station": "3"}), # T = 22 H2 performs hand hygiene
            (23, "Monitor patient", {"hcw": "H1", "donor": "D1", "station": "2"}), # T = 23 H1 monitors D1 at L
            (24, "Monitor patient", {"hcw": "H2", "donor": "D2", "station": "1"}), # T = 24 H2 monitors D2 at R
            (25, "Stop blood drawing machine", {"hcw": "H2", "machine": "ML", "station": "2"}), # T = 25 H2 stops blood drawing machine ML at L
            (26, "Remove needle", {"hcw": "H2", "donor": "D1", "station": "2"}), # T = 26 H2 removes needle from D1 at L
            (27, "Hand hygiene", {"hcw": "H2", "station": "3"}), # T = 27 H2 performs hand hygiene
            (28, "Donor check-out", {"donor": "D1", "station": "2"}), # T = 28 D1 checks out at L
            (29, "Take out samples", {"hcw": "H1", "machine": "ML", "station": "2"}), # T = 29 H1 takes out the samples from ML at L
            (30, "Stop blood drawing machine", {"hcw": "H2", "machine": "MR", "station": "1"}), # T = 30 H2 stops blood drawing machine MR at R
            (31, "Remove needle", {"hcw": "H2", "donor": "D2", "station": "1"}), # T = 31 H2 removes needle from D2 at R
            (32, "Hand hygiene", {"hcw": "H2", "station": "3"}), # T = 32 H2 performs hand hygiene
            (33, "Donor check-out", {"donor": "D2", "station": "1"}), # T = 33 D2 checks out at R
            (34, "Take out samples", {"hcw": "H2", "machine": "MR", "station": "1"}), # T = 34 H2 takes out the samples from MR at R
            (35, "Hand hygiene", {"hcw": "H1", "station": "3"}), # T = 35 H1 performs hand hygiene
            (36, "Hand hygiene", {"hcw": "H2", "station": "3"}), # T = 36 H2 performs hand hygiene
            (37, "HCW check-out", {"hcw": "H1", "station": "3"}), # T = 37 HCW H1 checks out
            (38, "HCW check-out", {"hcw": "H2", "station": "3"}), # T = 38 HCW H2 checks out
        ]
        self.index = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.index < len(self.steps):
            step = self.steps[self.index]
            self.index += 1
            return (step[:2] + (datetime.now(), step[2]))
        else:
            raise StopIteration
